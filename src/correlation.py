#!/usr/bin/env python3
"""Funções compartilhadas pelo mapper, combiner, reducer e execução local.

Cada estado estatístico é suficiente para combinar registros sem armazená-los
todos na memória. O mesmo formato é usado em todas as etapas distribuídas.
"""
import csv
import math
from typing import Dict, List, NamedTuple, Optional, Tuple


CSV_COLUMNS = (
    "X", "Y", "month", "day", "FFMC", "DMC", "DC", "ISI", "temp",
    "RH", "wind", "rain", "area",
)
# O enunciado chama a temperatura de coluna 9; índices Python começam em zero.
TEMPERATURE_INDEX = 8
# Variáveis quantitativas correlacionadas diretamente com a temperatura.
NUMERIC_COLUMNS = tuple(
    (index, name)
    for index, name in enumerate(CSV_COLUMNS)
    if index != TEMPERATURE_INDEX and name not in ("month", "day")
)
MONTHS = ("jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec")
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


class Statistics(NamedTuple):
    """Estado parcial suficiente para calcular a correlação de Pearson."""

    count: int  # número de registros deste estado
    mean_temperature: float  # média de temp
    mean_value: float  # média da variável corrente
    m2_temperature: float  # soma de (temp - média_temp)^2
    m2_value: float  # soma de (valor - média_valor)^2
    co_moment: float  # soma de (temp - média_temp) * (valor - média_valor)


def parse_record(
    line: str, line_number: int
) -> Optional[Tuple[float, List[Tuple[str, float]]]]:
    """Valida o registro completo e retorna temp e pares (variável, valor).

    Retorna None para linha vazia ou cabeçalho opcional. Erros de conteúdo são
    levantados para que o mapper falhe claramente, em vez de omitir dados.
    """
    if not line.strip():
        return None

    try:
        columns = next(csv.reader([line], strict=True))
    except (csv.Error, StopIteration) as error:
        raise ValueError(f"linha {line_number}: CSV inválido: {error}") from error

    if tuple(column.strip() for column in columns) == CSV_COLUMNS:
        return None  # Permite um cabeçalho opcional.
    if len(columns) != len(CSV_COLUMNS):
        raise ValueError(
            f"linha {line_number}: esperadas {len(CSV_COLUMNS)} colunas, "
            f"recebidas {len(columns)}"
        )

    # Converte todos os campos numéricos antes de construir qualquer saída.
    try:
        temperature = float(columns[TEMPERATURE_INDEX].strip())
        numeric_values = [
            (name, float(columns[index].strip()))
            for index, name in NUMERIC_COLUMNS
        ]
    except ValueError as error:
        raise ValueError(f"linha {line_number}: valor numérico inválido: {error}") from error
    if not math.isfinite(temperature) or any(
        not math.isfinite(value) for _, value in numeric_values
    ):
        raise ValueError(f"linha {line_number}: valor numérico não finito")

    month = columns[2].strip().lower()
    day = columns[3].strip().lower()
    if month not in MONTHS:
        raise ValueError(f"linha {line_number}: mês desconhecido: {month!r}")
    if day not in DAYS:
        raise ValueError(f"linha {line_number}: dia desconhecido: {day!r}")

    # month/day são nominais: cada categoria vira uma variável indicadora 0/1.
    values = numeric_values
    values.extend((f"month={category}", float(month == category)) for category in MONTHS)
    values.extend((f"day={category}", float(day == category)) for category in DAYS)
    return temperature, values


def single_observation(temperature: float, value: float) -> Statistics:
    """Estado inicial de um único par (temperatura, variável).

    Com uma observação, as médias são os próprios valores e os desvios
    quadráticos e o co-momento são zero. A combinação acumula a variação.
    """
    return Statistics(1, temperature, value, 0.0, 0.0, 0.0)


def merge_statistics(left: Statistics, right: Statistics) -> Statistics:
    """Combina dois grupos sem precisar reler os registros individuais.

    delta representa a diferença entre as médias dos grupos. O termo
    n_left*n_right/n_total ajusta desvios e co-momento para a média combinada.
    Esta propriedade permite ao Hadoop combinar em qualquer árvore/ordem.
    """
    if left.count == 0:
        return right
    if right.count == 0:
        return left

    count = left.count + right.count
    temperature_delta = right.mean_temperature - left.mean_temperature
    value_delta = right.mean_value - left.mean_value
    cross_weight = left.count * right.count / count
    return Statistics(
        count,
        left.mean_temperature + temperature_delta * right.count / count,
        left.mean_value + value_delta * right.count / count,
        left.m2_temperature + right.m2_temperature
        + temperature_delta * temperature_delta * cross_weight,
        left.m2_value + right.m2_value + value_delta * value_delta * cross_weight,
        left.co_moment + right.co_moment
        + temperature_delta * value_delta * cross_weight,
    )


def serialize_statistics(statistics: Statistics) -> str:
    """Codifica os seis campos como CSV compacto para transportar no valor."""
    return ",".join(
        (str(statistics.count),)
        + tuple(
            format(value, ".17g")
            for value in statistics[1:]
        )
    )


def parse_statistics(value: str) -> Statistics:
    """Valida e reconstrói o estado emitido pelo mapper/combiner."""
    fields = value.split(",")
    if len(fields) != 6:
        raise ValueError(f"estado estatístico inválido: {value!r}")
    try:
        statistics = Statistics(int(fields[0]), *(float(field) for field in fields[1:]))
    except ValueError as error:
        raise ValueError(f"estado estatístico inválido: {value!r}") from error
    if statistics.count < 1 or not all(math.isfinite(item) for item in statistics[1:]):
        raise ValueError(f"estado estatístico inválido: {value!r}")
    return statistics


def correlation(statistics: Statistics) -> Optional[float]:
    """Calcula Pearson no estado global; None significa variância zero."""
    denominator_squared = statistics.m2_temperature * statistics.m2_value
    if denominator_squared <= 0.0:
        return None
    result = statistics.co_moment / math.sqrt(denominator_squared)
    # Evita mostrar, por arredondamento numérico, um Pearson fora do intervalo.
    return max(-1.0, min(1.0, result))


def format_result(feature: str, statistics: Statistics) -> str:
    """Formata a saída final: variável, número de pares e correlação."""
    result = correlation(statistics)
    formatted = "NA" if result is None else f"{result:.6f}"
    return f"{feature}\tquantidade={statistics.count}\tcorrelacao={formatted}"


def merge_by_feature(
    groups: Dict[str, Statistics], feature: str, statistics: Statistics
) -> None:
    """Insere ou combina estado no acumulador indexado por variável."""
    if feature in groups:
        groups[feature] = merge_statistics(groups[feature], statistics)
    else:
        groups[feature] = statistics
