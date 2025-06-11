#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para download de relatórios de Carteira Diária do BTG Pactual Asset Management.

Autor: Álvaro - Equipe Data Analytics - Catalise Investimentos
Data: 29/05/2025
Versão: 1.1.3 (corrigido saída de métricas JSON)
"""

import requests
import datetime
import os
import zipfile
import argparse
import traceback
import shutil
import time
import json
import re
import sys
from pathlib import Path
from dotenv import load_dotenv


ROOT_PATH = Path(__file__).resolve().parents[2]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from utils.date_utils import get_business_day
from utils.logging_utils import Log, LogLevel
from utils.backoff_utils import with_backoff_jitter


LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
Log.set_log_file(str(LOGS_DIR / f"api_faas_portfolio_{datetime.datetime.now().strftime('%Y%m%d')}.log"))
logger = Log.get_logger(__name__)


env_path = ROOT_PATH / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

AUTH_URL       = os.getenv("AUTH_URL")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SCOPE          = os.getenv("SCOPE_CARTEIRA")
GRANT_TYPE     = os.getenv("GRANT_TYPE", "client_credentials")
PORTFOLIO_URL  = os.getenv("PORTFOLIO_URL")
TICKET_URL     = os.getenv("TICKET_URL")
DOWNLOAD_PATH  = os.getenv("BTG_REPORT_PATH")

@with_backoff_jitter(max_attempts=3, base_wait=2.0, jitter=0.3)
def get_token() -> str:
    """Obtém o token de autenticação da API BTG."""
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": GRANT_TYPE,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    logger.info("Obtendo token de autenticação...")
    resp = requests.post(AUTH_URL, headers=headers, data=data, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error(f"Falha ao obter token: {resp.status_code} – {resp.text}")
        raise
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"access_token não retornado: {resp.text}")
    logger.info("Token obtido com sucesso.")
    return token

@with_backoff_jitter(max_attempts=3, base_wait=2.0, jitter=0.3)
def request_portfolio_ticket(token: str, date_str: str) -> str:
    """Solicita ticket POST /reports/Portfolio."""
    url = PORTFOLIO_URL
    headers = {
        "Accept": "application/json",
        "X-SecureConnect-Token": token,
        "Content-Type": "application/json-patch+json"
    }
    payload = {
        "contract": {
            "startDate": f"{date_str}T00:00:00.000Z",
            "endDate":   f"{date_str}T23:59:59.000Z",
            "typeReport": 10,
            "fundName": ""
        },
        "pageSize": 100,
        "webhookEndpoint": ""
    }
    logger.info(f"Solicitando ticket POST {url} para {date_str}")
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error(f"Falha ao solicitar ticket: {resp.status_code} – {resp.text}")
        raise
    ticket = resp.json().get("ticket")
    if not ticket:
        raise ValueError(f"ticket não retornado: {resp.text}")
    logger.info(f"Ticket obtido: {ticket}")
    return ticket

@with_backoff_jitter(max_attempts=6, base_wait=8.0, jitter=0.4)
def download_zip(token: str, ticket: str, raw_dir: Path, page_number: int = 1) -> Path:
    """Baixa o ZIP via GET /reports/Ticket."""
    url = TICKET_URL
    params = {"ticketId": ticket, "pageNumber": page_number}
    headers = {
        "X-SecureConnect-Token": token,
        "Accept": "application/octet-stream"
    }

    logger.info(f"Download GET {url}?ticketId={ticket}&pageNumber={page_number}")
    resp = requests.get(url, headers=headers, params=params, stream=True, timeout=60)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error(f"Falha no download do ZIP: {resp.status_code} – {resp.text}")
        raise

    content_type = resp.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        
        try:
            data = resp.json()
            raise ValueError(f"Resposta inesperada JSON em vez de ZIP: {data}")
        except json.JSONDecodeError:
            
            pass

    cd = resp.headers.get("content-disposition", "")
    m = re.search(r"filename\*=UTF-8''(.+)", cd) or re.search(r'filename="([^"]+)"', cd)
    fname = m.group(1) if m else f"{ticket}.zip"

    raw_dir.mkdir(parents=True, exist_ok=True)
    fpath = raw_dir / fname

    # Salva em disco em chunks
    with open(fpath, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # Verificar se parece um ZIP válido
    try:
        with zipfile.ZipFile(fpath, 'r'):
            pass
    except zipfile.BadZipFile:
        logger.error(f"Arquivo baixado não é um ZIP válido: {fpath}")
        raise zipfile.BadZipFile("Conteúdo recebido não é um arquivo ZIP válido.")

    logger.info(f"ZIP salvo em: {fpath}")
    return fpath

def extract_zip(zip_path: Path, out_dir: Path, remove_zip: bool = True) -> int:
    """Extrai CSV/XLSX de ZIP e retorna a quantidade de arquivos extraídos."""
    logger.info(f"Extraindo '{zip_path}' → '{out_dir}'")
    out_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            members = [m for m in z.namelist() if m.lower().endswith(('.csv', '.xlsx'))]
            for member in members:
                if not member.startswith("__MACOSX/"):
                    target = out_dir / Path(member).name
                    with z.open(member) as src, open(target, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                    count += 1
    except zipfile.BadZipFile:
        logger.error("Arquivo não é um ZIP válido.")
        raise

    if remove_zip:
        try:
            zip_path.unlink()
            logger.info(f"ZIP removido: {zip_path}")
        except Exception as e:
            logger.warning(f"Falha ao remover {zip_path}: {e}")

    logger.info(f"Extraídos {count} arquivos para {out_dir}")
    return count

def main(date_str: str, base_output: Path) -> int:
    """
    Fluxo principal com saída JSON corrigida para o orquestrador.
    """
    logger.info(f"Processando data: {date_str}")
    raw_dir = base_output / "raw"
    extracted_dir = base_output / "extracted" / date_str

    raw_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    
    try:
        token = get_token()
        ticket = request_portfolio_ticket(token, date_str)
        zip_file = download_zip(token, ticket, raw_dir)
        total_files = extract_zip(zip_file, extracted_dir)
        
        duracao = round(time.time() - start_time, 3)

        # Calcular métricas reais
        tamanho_bytes = zip_file.stat().st_size if zip_file.exists() else 0
        
        # Métricas de sucesso em formato JSON
        metrics = {
            "status": "SUCESSO",
            "total_fundos": total_files,
            "tamanho_total": tamanho_bytes,
            "duracao_segundos": duracao,
            "erros": []
        }
        
        logger.info(f"Extração concluída com sucesso: {total_files} arquivos em {duracao}s")
        print(json.dumps(metrics, ensure_ascii=False))
        return total_files

    except Exception as e:
        duracao = round(time.time() - start_time, 3)
        error_msg = str(e)
        
        logger.error(f"Erro durante extração: {error_msg}")
        
        # Métricas de falha em formato JSON
        metrics = {
            "status": "FALHA",
            "total_fundos": 0,
            "tamanho_total": 0,
            "duracao_segundos": duracao,
            "erros": [error_msg]
        }
        
        print(json.dumps(metrics, ensure_ascii=False))
        return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Carteira BTG')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--n-days', type=int, help='Dias úteis atrás')
    group.add_argument('--date', type=str, help='Data no formato YYYY-MM-DD')
    parser.add_argument(
        '--output-dir-base',
        type=str,
        default=os.getenv('OUTPUT_DIR_BASE', 'output'),
        help='Diretório base onde ficarão as pastas "raw" e "extracted"'
    )
    args = parser.parse_args()

    try:
        if args.n_days is not None:
            d = get_business_day(n_days=args.n_days).strftime('%Y-%m-%d')
        elif args.date:
            datetime.datetime.strptime(args.date, '%Y-%m-%d')
            d = args.date
        else:
            # Pega dia útil anterior
            d = get_business_day(datetime.date.today(), 1).strftime('%Y-%m-%d')
    except Exception as e:
        logger.error("Formato de data inválido. Use YYYY-MM-DD ou --n-days.")
        error_metrics = {
            "status": "FALHA",
            "total_fundos": 0,
            "tamanho_total": 0,
            "duracao_segundos": 0,
            "erros": [f"Data inválida: {str(e)}"]
        }
        print(json.dumps(error_metrics, ensure_ascii=False))
        sys.exit(1)

    base_out = Path(args.output_dir_base)
    result = main(d, base_out)
    sys.exit(0 if result > 0 else 1)