#!/usr/bin/env python3
"""Reducer Hadoop Streaming: combina estados globais e calcula Pearson."""
import sys

from correlation import format_result, merge_statistics, parse_statistics


def main() -> None:
    # O sort/shuffle padrão do Hadoop entrega as chaves agrupadas.
    current_feature = None
    current_statistics = None
    emitted = False

    try:
        for line_number, line in enumerate(sys.stdin, start=1):
            fields = line.rstrip("\r\n").split("\t", 1)
            if len(fields) != 2:
                raise ValueError(f"linha {line_number}: entrada do reducer inválida")
            feature, value = fields
            statistics = parse_statistics(value)

            # Ao mudar a chave, todos os estados da variável anterior chegaram.
            # Só então emitimos sua correlação global e iniciamos a próxima.
            if current_feature is not None and feature != current_feature:
                print(format_result(current_feature, current_statistics))
                emitted = True
                current_statistics = None
            current_feature = feature
            current_statistics = (
                statistics if current_statistics is None
                else merge_statistics(current_statistics, statistics)
            )
    except ValueError as error:
        # Entrada inconsistente deve falhar, não produzir uma saída enganosa.
        print(f"Erro no reducer: {error}", file=sys.stderr)
        sys.exit(1)

    if current_feature is not None:
        print(format_result(current_feature, current_statistics))
        emitted = True
    if not emitted:
        sys.exit("Nenhuma observação válida foi encontrada.")


if __name__ == "__main__":
    main()
