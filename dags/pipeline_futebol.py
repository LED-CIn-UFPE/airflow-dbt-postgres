from datetime import datetime
from airflow.decorators import dag, task

def processar_partidas(partidas: list[dict]) -> list[dict]:
    resultado = []
    for p in partidas:
        if p.get("status") == "FINISHED":
            home = p.get("score", {}).get("fullTime", {}).get("home", 0)
            away = p.get("score", {}).get("fullTime", {}).get("away", 0)
            resultado.append({
                "id": p["id"],
                "home_team": p["homeTeam"]["name"],
                "away_team": p["awayTeam"]["name"],
                "placar": f"{home} x {away}",
                "vencedor": p["homeTeam"]["name"] if home > away else p["awayTeam"]["name"] if away > home else "Empate"
            })
    return resultado

@dag(
    dag_id="pipeline_futebol_broken",
    start_date=datetime.now(),
    schedule="@daily",
    catchup=False,
    tags=["mentoria", "exercicio"],
)
def pipeline_futebol():

    @task
    def extrair_partidas() -> list[dict]:
        """Simula a extração de partidas da API football-data.org"""
        import time
        time.sleep(1)
        return [
            {
                "id": 1,
                "status": "FINISHED",
                "homeTeam": {"name": "Flamengo"},
                "awayTeam": {"name": "Fluminense"},
                "score": {"fullTime": {"home": 2, "away": 1}},
                "dados_extras": list(range(100_000)),
            },
            {
                "id": 2,
                "status": "SCHEDULED",
                "homeTeam": {"name": "Vasco"},
                "awayTeam": {"name": "Botafogo"},
                "score": {"fullTime": {"home": None, "away": None}},
                "dados_extras": list(range(100_000)),
            },
        ]

    @task
    def transformar_partidas(partidas: list[dict]) -> list[dict]:
        """Processa e filtra as partidas extraídas"""
        return processar_partidas(partidas)

    @task
    def salvar_partidas(partidas: list[dict]) -> None:
        """Simula o salvamento das partidas processadas"""
        print(f"Salvando {len(partidas)} partidas processadas...")
        for p in partidas:
            print(f"  → {p['home_team']} {p['placar']} {p['away_team']}")

    @task
    def gerar_relatorio(partidas: list[dict]) -> None:
        """Gera um relatório simples das partidas do dia"""
        print("=== RELATÓRIO DO DIA ===")
        print(f"Total de partidas finalizadas: {len(partidas)}")

    partidas_raw = extrair_partidas()
    partidas_proc = transformar_partidas(partidas_raw)
    relatorio = gerar_relatorio(partidas_proc)
    salvar = salvar_partidas(partidas_proc)

    relatorio >> salvar

    salvar >> notificar_time


pipeline_futebol()