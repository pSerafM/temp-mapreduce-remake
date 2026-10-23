#!/usr/bin/env python3
"""Mapper Hadoop Streaming: valida cada registro e emite estados de Pearson."""
import sys
from typing import List, Tuple

from correlation import parse_record, serialize_statistics, single_observation


def map_line(line: str, line_number: int = 1) -> List[Tuple[str, str]]:
    """Mapeia um registro completo para pares chave/estado.

    A função primeiro valida e interpreta toda a linha; só depois cria as
    saídas. Isso impede que um registro malformado contribua para apenas parte
    das variáveis calculadas.
    """
    record = parse_record(line, line_number)
    if record is None:
        return []

    temperature, values = record
    # Cada variável recebe o estado de uma observação (temp, valor).
    # O mapper não calcula Pearson: isso só é correto depois de combinar
    # observações de todos os mappers.
    return [
        (feature, serialize_statistics(single_observation(temperature, value)))
        for feature, value in values
    ]


def main() -> None:
    # Hadoop Streaming conecta stdin/stdout dos processos ao fluxo do job.
    for line_number, line in enumerate(sys.stdin, start=1):
        try:
            mapped = map_line(line, line_number)
        except ValueError as error:
            # Código de saída 1 sinaliza falha da tarefa; não esconder registro.
            print(f"Erro no mapper: {error}", file=sys.stderr)
            sys.exit(1)
        # A chave precede TAB; o valor serializado pode ser agregado adiante.
        for feature, statistics in mapped:
            print(f"{feature}\t{statistics}")


if __name__ == "__main__":
    main()
