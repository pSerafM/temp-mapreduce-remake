#!/usr/bin/env python3
"""Simula localmente Map -> Combiner -> Shuffle/Sort -> Reduce, sem Hadoop."""
import sys
from collections import defaultdict
from pathlib import Path

from mapper import map_line
from correlation import format_result, merge_by_feature, merge_statistics, parse_statistics

CHUNK_SIZE = 50


def main() -> None:
    # O caminho padrão independe da pasta de onde o usuário executa o comando.
    default_input = Path(__file__).resolve().parent.parent / "forestfireinput.csv"
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_input
    if not path.exists():
        sys.exit(f"Arquivo não encontrado: {path.resolve()}")

    # shuffle guarda os estados parciais que viriam de diferentes mappers.
    shuffle = defaultdict(list)
    # mapped_chunk representa um mapper/combiner local de até CHUNK_SIZE linhas.
    mapped_chunk = {}
    lines_in_chunk = 0

    def flush() -> None:
        # Combina os estados do bloco antes de enviá-los ao shuffle simulado.
        # A última partição também é enviada, mesmo se tiver menos de 50 linhas.
        for feature, statistics in mapped_chunk.items():
            shuffle[feature].append(statistics)
        mapped_chunk.clear()

    with path.open(encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            try:
                pairs = map_line(line, line_number)
            except ValueError as error:
                sys.exit(f"Erro ao processar CSV: {error}")
            for feature, value in pairs:
                # O estado da mesma variável é combinado dentro do bloco.
                merge_by_feature(
                    mapped_chunk, feature, parse_statistics(value)
                )
            lines_in_chunk += 1
            if lines_in_chunk == CHUNK_SIZE:
                flush()
                lines_in_chunk = 0

    if mapped_chunk:      # último bloco (menor que 50) também passa pelo Combiner
        flush()

    if not shuffle:
        sys.exit("Nenhuma observação válida foi encontrada.")

    # Shuffle junta os estados de blocos diferentes pela chave/variável.
    # O reducer simulado combina todos os blocos antes de calcular Pearson.
    print("variavel\tquantidade\tcorrelacao_pearson")
    for feature in sorted(shuffle):
        statistics = None
        for partial in shuffle[feature]:
            statistics = (
                partial if statistics is None
                else merge_statistics(statistics, partial)
            )
        print(format_result(feature, statistics))


if __name__ == "__main__":
    main()
