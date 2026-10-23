#!/usr/bin/env python3
"""Combiner Hadoop Streaming: agrega estados parciais antes do reducer."""
import sys

from correlation import merge_by_feature, parse_statistics, serialize_statistics


def main() -> None:
    # O Hadoop fornece registros no formato chave<TAB>estado.
    # A chave identifica a variável cuja correlação está sendo calculada.
    partials = {}
    try:
        for line_number, line in enumerate(sys.stdin, start=1):
            fields = line.rstrip("\r\n").split("\t", 1)
            if len(fields) != 2:
                raise ValueError(f"linha {line_number}: saída do mapper inválida")
            feature, value = fields
            # O valor pode vir do mapper ou de outra execução do combiner.
            # Como ambos usam o mesmo formato, esta agregação pode ser repetida.
            merge_by_feature(partials, feature, parse_statistics(value))
    except ValueError as error:
        # Vai para stderr; stdout fica reservado ao protocolo do Hadoop.
        print(f"Erro no combiner: {error}", file=sys.stderr)
        sys.exit(1)

    # Emite apenas um estado resumido por variável vista neste combiner.
    for feature in sorted(partials):
        print(f"{feature}\t{serialize_statistics(partials[feature])}")


if __name__ == "__main__":
    main()
