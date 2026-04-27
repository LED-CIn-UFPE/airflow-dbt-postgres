# ⚽ Mentoria: Airflow + dbt
Pipeline de dados de futebol usando Apache Airflow, dbt Core e PostgreSQL.

**Fonte:** [football-data.org](https://www.football-data.org/) — API gratuita com dados de partidas, competições e times.

**Stack:** Apache Airflow 3 (Astro CLI) · dbt Core 1.11 · PostgreSQL 15 · Docker

---

## Pré-requisitos

- [Docker](https://docs.docker.com/engine/install/) instalado e rodando
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) instalado
- Conta gratuita na [football-data.org](https://www.football-data.org/) para obter a API key

### Instalar o Astro CLI (Linux)

```bash
curl -sSL install.astronomer.io | sudo bash -s
astro version  # confirmar instalação
```

### Instalar o Astro CLI (PC do CIn)

#### 1. Baixar o binário diretamente do GitHub

```bash
# Entrar na pasta temporária
cd /tmp

# Baixar a versão mais recente do Astro CLI (Linux 64-bit)
curl -L https://github.com/astronomer/astro-cli/releases/download/v1.41.0/astro_1.41.0_linux_amd64.tar.gz -o astro.tar.gz

# Extrair o conteúdo
tar -xvzf astro.tar.gz
```

#### 2. Mover pra sua pasta local

```bash
mv astro ~/.local/bin/
```

#### 3. Configurar o path

```bash
# Adiciona a pasta ao PATH no arquivo de configuração do terminal
echo 'export PATH=$PATH:$HOME/.local/bin' >> ~/.bashrc
```

#### 4. Testar

```bash
astro version
```

---

## Setup

### 1. Clone o repositório

```bash
git clone <url-do-repo>
cd airflow-dbt-postgres
```

### 2. Configure o profiles.yml do dbt

O `profiles.yml` contém as credenciais de conexão do dbt com o banco — por isso não é commitado. Crie o seu a partir do template:

```bash
cp include/dbt/profiles.yml.example include/dbt/profiles.yml
```

> Não é necessário alterar nada — as credenciais já batem com o Postgres que sobe via Docker.

### 4. Suba o ambiente

```bash
astro dev start
```

Esse comando builda a imagem e sobe todos os serviços:

| Serviço | URL / Porta | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Postgres Airflow (metadata) | localhost:5432 | postgres / postgres |
| Postgres Data (warehouse) | localhost:5433 | airflow / airflow / football_db |

> Na primeira vez o build pode levar alguns minutos.

### 5. Verifique a conexão do dbt

```bash
astro dev bash
cd include/dbt
dbt debug --profiles-dir .
# All checks passed! ✓
```

---

## Estrutura do projeto

```
airflow-dbt-postgres/
├── dags/
│   └── pipeline_futebol.py        # DAG principal
├── include/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml.example    # template — copie para profiles.yml
│       ├── profiles.yml            # não commitado — credenciais
│       └── models/
│           ├── staging/
│           │   ├── _sources.yml
│           │   └── stg_matches.sql
│           ├── intermediate/
│           │   └── int_matches_enriched.sql
│           └── marts/
│               └── fct_matches.sql
├── plugins/
├── docker-compose.override.yml    # adiciona o Postgres de dados
├── Dockerfile
├── packages.txt
├── requirements.txt
├── .env.example
└── .env                           # não commitado — credenciais
```

---

## Comandos úteis

### Airflow

```bash
astro dev start       # sobe o ambiente
astro dev stop        # para o ambiente
astro dev restart     # reinicia (necessário após mudar requirements.txt ou packages.txt)
astro dev logs --scheduler  # acompanha logs do scheduler
```

### dbt (dentro do container)

```bash
astro dev bash                        # entra no container do scheduler
cd include/dbt

dbt debug --profiles-dir .            # testa conexão
dbt run --profiles-dir .              # executa todos os models
dbt test --profiles-dir .             # roda os testes
dbt build --profiles-dir .            # run + test em sequência
dbt docs generate --profiles-dir .    # gera documentação
dbt docs serve --profiles-dir .       # abre documentação no browser
```

### Postgres Data

```bash
# conectar de fora do Docker
psql -h localhost -p 5433 -U airflow -d football_db
```
