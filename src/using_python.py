from __future__ import annotations
from csv import reader
from collections import defaultdict, Counter
from tqdm import tqdm
from pathlib import Path
import logging
import time
from typing import Dict, Tuple, Union

# CONFIGURAÇÃO DE LOG E CONSTANTES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

NUMERO_DE_LINHAS: int = 100_000_000

# CLASSE PRINCIPAL

class TemperatureProcessor:
    """
    Classe responsável por processar grandes volumes de dados de temperatura
    e calcular as estatísticas mínimas, médias e máximas para cada estação.

    Attributes:
        path (Path): Caminho para o arquivo de dados.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("O caminho do arquivo deve ser do tipo str ou Path.")
        self.path = Path(path)

        if not self.path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {self.path}")

        self.minimas = defaultdict(lambda: float("inf"))
        self.maximas = defaultdict(lambda: float("-inf"))
        self.somas = defaultdict(float)
        self.medicoes = Counter()

    # MÉTODO PRINCIPAL

    def processar(self) -> Dict[str, str]:
        """
        Lê o arquivo de medições, calcula as estatísticas e retorna os resultados formatados.
        Returns:
            Dict[str, str]: Dicionário com a estação e suas métricas formatadas (min/média/max).
        """
        logging.info(f"Iniciando processamento do arquivo: {self.path}")

        try:
            with open(self.path, "r", encoding="utf-8") as file:
                csv_reader = reader(file, delimiter=';')
                for row in tqdm(csv_reader, total=NUMERO_DE_LINHAS, desc="Processando linhas"):
                    try:
                        nome_station, temperatura = self._parse_row(row)
                        self._atualizar_estatisticas(nome_station, temperatura)
                    except (ValueError, IndexError) as e:
                        logging.warning(f"Linha ignorada (inválida): {row} -> {e}")
                        continue
        except Exception as e:
            logging.error(f"Erro ao processar o arquivo: {e}")
            raise

        return self._gerar_resultados()

    # MÉTODOS AUXILIARES

    def _parse_row(self, row: list[str]) -> Tuple[str, float]:
        """
        Faz a validação e conversão dos dados de uma linha do CSV.
        """
        if len(row) < 2:
            raise IndexError("Linha incompleta, esperado 2 colunas.")
        nome_station = str(row[0]).strip()
        temperatura = float(row[1])
        return nome_station, temperatura

    def _atualizar_estatisticas(self, nome: str, temperatura: float) -> None:
        """
        Atualiza os valores mínimos, máximos, soma e contador de medições por estação.
        """
        self.medicoes.update([nome])
        self.minimas[nome] = min(self.minimas[nome], temperatura)
        self.maximas[nome] = max(self.maximas[nome], temperatura)
        self.somas[nome] += temperatura

    def _gerar_resultados(self) -> Dict[str, str]:
        """
        Calcula média, organiza e formata os resultados para exibição.
        """
        logging.info("Calculando estatísticas e ordenando resultados...")

        resultados = {}
        for station, qtd_medicoes in self.medicoes.items():
            if qtd_medicoes == 0:
                continue
            mean_temp = self.somas[station] / qtd_medicoes
            resultados[station] = (
                self.minimas[station],
                mean_temp,
                self.maximas[station]
            )

        sorted_results = dict(sorted(resultados.items()))
        return {
            station: f"{min_t:.1f}/{mean_t:.1f}/{max_t:.1f}"
            for station, (min_t, mean_t, max_t) in sorted_results.items()
        }

# BLOCO PRINCIPAL DE EXECUÇÃO

if __name__ == "__main__":
    path_do_txt = Path("data/measurements.txt")

    logging.info("Iniciando o processamento do arquivo.")
    start_time = time.time()

    try:
        processor = TemperatureProcessor(path_do_txt)
        resultados = processor.processar()

        for station, metrics in resultados.items():
            print(f"{station}: {metrics}")

        logging.info(f"Processamento concluído em {time.time() - start_time:.2f} segundos.")
    except Exception as e:
        logging.exception(f"Falha crítica no processamento: {e}")