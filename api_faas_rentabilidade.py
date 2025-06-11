#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para download de relatórios de rentabilidade do BTG Pactual Asset Management

Autor: Álvaro – Equipe Data Analytics – Catalise Investimentos
Data: 01/06/2025
Versão: 1.1.0 (Refatoração para uso de utilitários e padronização de desenvolvimento)
"""

import requests
import datetime
import os
import time
import argparse
import traceback
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

# Ajustar ROOT_DIR para garantir que 'utils' seja encontrado
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv
from utils.logging_utils import Log, LogLevel
from utils.backoff_utils import with_backoff_jitter
from utils.date_utils import get_business_day

# Configuração de logs
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
hoje_str = datetime.datetime.now().strftime("%Y%m%d")
Log.set_log_file(str(LOGS_DIR / f"api_faas_rentabilidade_{hoje_str}.log"))
logger = Log.get_logger(__name__)

# Carrega variáveis de ambiente
dotenv_path = ROOT_DIR / ".env"
load_dotenv(dotenv_path=dotenv_path)

AUTH_URL              = os.getenv("AUTH_URL")
TICKET_URL            = os.getenv("TICKET_URL")
RENTABILIDADE_URL     = os.getenv("RENTABILIDADE_URL")
CLIENT_ID             = os.getenv("CLIENT_ID")
CLIENT_SECRET         = os.getenv("CLIENT_SECRET")
SCOPE                 = os.getenv("SCOPE_PATRIMONIO")
DEFAULT_DOWNLOAD_PATH = os.getenv("BTG_RENTABILIDADE")

if not AUTH_URL or not TICKET_URL or not RENTABILIDADE_URL \
   or not CLIENT_ID or not CLIENT_SECRET or not SCOPE or not DEFAULT_DOWNLOAD_PATH:
    logger.error("Variáveis de ambiente não completamente configuradas.")
    sys.exit(1)

@with_backoff_jitter(max_attempts=3, base_wait=2.0)
def get_token() -> str:
    """Obtém o token de autenticação da API BTG."""
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    resp = requests.post(AUTH_URL, headers=headers, data=data, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error(f"Erro ao obter token: {resp.status_code} – {resp.text}")
        raise
    token_json = resp.json()
    token = token_json.get("access_token")
    if not token:
        logger.error(f"Token não encontrado na resposta: {resp.text}")
        raise RuntimeError("Token ausente na resposta da API.")
    logger.info("Token obtido com sucesso.")
    return token

def request_ticket(data_ref: datetime.date) -> str:
    """Solicita um ticket para gerar o relatório de rentabilidade no BTG."""
    token = get_token()

    headers = {
        "Accept": "application/json",
        "X-SecureConnect-Token": token,
        "Content-Type": "application/json"
    }

    payload = {
        "contract": {
            "startDate": f"{data_ref.strftime('%Y-%m-%d')}T00:00:00.000Z",
            "endDate":   f"{data_ref.strftime('%Y-%m-%d')}T23:59:59.000Z",
            "indexers": ["CDIE"],
            "fundName": ""
        },
        "pageSize": 100,
        "webhookEndpoint": ""
    }

    logger.info(f"[request_ticket] payload: {json.dumps(payload, ensure_ascii=False)}")

    resp = requests.post(RENTABILIDADE_URL, headers=headers, json=payload, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error(f"Falha ao solicitar ticket (rentabilidade): {resp.status_code} – {resp.text}")
        raise

    ticket = resp.json().get("ticket")
    if not ticket:
        raise ValueError(f"ticket não retornado no corpo da resposta: {resp.text}")

    logger.info(f"[request_ticket] Ticket obtido: {ticket}")
    return ticket

def download_report_json(ticket: str, page_number: int, output_path: Path, max_attempts: int = 6, wait_time: int = 15) -> bool:
    """
    Faz GET em TICKET_URL até receber um JSON final.
    Versão corrigida com renovação de token quando necessário.
    
    Returns:
        True se conseguiu baixar o JSON, False se falhou após todas as tentativas
    """
    url = f"{TICKET_URL}?ticketId={ticket}&pageNumber={page_number}"
    
    logger.info(f"[download_report_json] Iniciando download para ticket {ticket}, página {page_number}")
    logger.info(f"[download_report_json] Máximo de {max_attempts} tentativas com intervalo de {wait_time}s")

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"[download_report_json] Tentativa {attempt}/{max_attempts}")
            
            # CORREÇÃO: Renovar token a cada tentativa para evitar expiração
            try:
                token = get_token()
            except Exception as e:
                logger.error(f"[download_report_json] Erro ao renovar token na tentativa {attempt}: {e}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue
                else:
                    return False
            
            headers = {
                "Accept": "application/json",
                "X-SecureConnect-Token": token  # Usar token renovado
            }
            
            resp = requests.get(url, headers=headers, timeout=60)
            
            # CORREÇÃO: Tratar especificamente erro 401 (token expirado)
            if resp.status_code == 401:
                logger.warning(f"[download_report_json] Token expirado (401) na tentativa {attempt}, renovando...")
                if attempt < max_attempts:
                    time.sleep(2)  # Espera menor para tentar com token novo
                    continue
                else:
                    logger.error(f"[download_report_json] Falha de autenticação após {max_attempts} tentativas")
                    return False
            
            if resp.status_code != 200:
                logger.warning(f"[download_report_json] Status HTTP {resp.status_code} na tentativa {attempt}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[download_report_json] Falha após {max_attempts} tentativas. Status: {resp.status_code}")
                    return False
            
            content_type = resp.headers.get("Content-Type", "").lower()
            
            if 'application/json' in content_type:
                try:
                    data = resp.json()
                    
                    # Verificar se ainda está processando
                    if isinstance(data, dict) and data.get("result") == "Processando":
                        logger.info(f"[download_report_json] Relatório em processamento. Tentativa {attempt}/{max_attempts}. Aguardando {wait_time}s.")
                        if attempt < max_attempts:
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"[download_report_json] Timeout: relatório ainda processando após {max_attempts} tentativas")
                            return False
                    
                    # Se chegou aqui, é JSON final (com dados)
                    filename = output_path / f"{ticket}_p{page_number}.json"
                    
                    # Verificar se há dados válidos
                    if isinstance(data, dict) and "result" in data:
                        result = data.get("result", [])
                        if isinstance(result, list) and len(result) > 0:
                            with open(filename, "w", encoding="utf-8") as f:
                                json.dump(data, f, ensure_ascii=False, indent=2)
                            logger.info(f"[download_report_json] JSON com dados salvo em: {filename} ({len(result)} fundos)")
                            return True
                        else:
                            logger.warning(f"[download_report_json] JSON recebido mas sem dados válidos na página {page_number}")
                            # Salvar mesmo assim para debug
                            with open(filename, "w", encoding="utf-8") as f:
                                json.dump(data, f, ensure_ascii=False, indent=2)
                            return True
                    else:
                        # JSON sem estrutura esperada, salvar e continuar
                        filename = output_path / f"{ticket}_p{page_number}.json"
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        logger.info(f"[download_report_json] JSON salvo (estrutura inesperada): {filename}")
                        return True
                        
                except json.JSONDecodeError as e:
                    logger.error(f"[download_report_json] Erro ao decodificar JSON na tentativa {attempt}: {e}")
                    if attempt < max_attempts:
                        time.sleep(wait_time)
                        continue
                    else:
                        return False
            else:
                logger.error(f"[download_report_json] Esperava JSON, veio: {content_type}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue
                else:
                    return False
                    
        except requests.RequestException as e:
            logger.error(f"[download_report_json] Erro de rede na tentativa {attempt}: {e}")
            if attempt < max_attempts:
                time.sleep(wait_time)
                continue
            else:
                return False
        except Exception as e:
            logger.error(f"[download_report_json] Erro inesperado na tentativa {attempt}: {e}")
            if attempt < max_attempts:
                time.sleep(wait_time)
                continue
            else:
                return False

    logger.error(f"[download_report_json] Falha após {max_attempts} tentativas")
    return False

def main():
    parser = argparse.ArgumentParser(
        description="API FAAS Rentabilidade BTG – ETL de Rentabilidade"
    )
    parser.add_argument(
        "--json-dir", type=str, required=True,
        help="Diretório onde os JSONs de rentabilidade serão salvos."
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Data de referência (YYYY-MM-DD). Se não informada, usa ontem útil."
    )
    args = parser.parse_args()

    start_time = time.time()

    try:
        # 1) Definir data de referência
        if args.date:
            data_ref = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            hoje = datetime.date.today()
            data_ref = get_business_day(hoje, 1)
    except Exception as e:
        logger.error(f"Data de referência inválida: {e}")
        sys.exit(1)

    logger.info(f"[main] Data de referência: {data_ref}")

    # 2) Preparar diretório para salvar JSONs
    pasta_jsons = Path(args.json_dir)
    if pasta_jsons.exists():
        # Limpar qualquer JSON antigo
        for arq in pasta_jsons.glob("*.json"):
            try:
                arq.unlink()
            except Exception:
                pass
    pasta_jsons.mkdir(parents=True, exist_ok=True)

    # 3) Solicitar o ticket
    try:
        ticket = request_ticket(data_ref)
    except Exception as e:
        logger.error(f"[main] ERRO CRÍTICO: Erro ao solicitar ticket: {e}")
        duracao = round(time.time() - start_time, 3)
        fallback = {
            "status": "FALHA",
            "total_jsons": 0,
            "data_referencia": data_ref.strftime("%Y-%m-%d"),
            "output_path": str(pasta_jsons),
            "erros": [str(e)],
            "duracao_segundos": duracao
        }
        print(json.dumps(fallback, ensure_ascii=False))
        sys.exit(1)

    # 4) Baixar as páginas de JSON (aumentei para 5 páginas e melhorei a lógica)
    all_json_files = []
    max_pages = 5  # Aumentado para permitir mais páginas
    
    for page in range(1, max_pages + 1):
        logger.info(f"[main] Tentando baixar página {page}")
        success = download_report_json(ticket, page, pasta_jsons)
        
        if success:
            arquivo_baixado = pasta_jsons / f"{ticket}_p{page}.json"
            if arquivo_baixado.exists():
                all_json_files.append(str(arquivo_baixado))
                logger.info(f"[main] Página {page} baixada com sucesso")
                
                # Verificar se há mais páginas analisando o conteúdo
                try:
                    with open(arquivo_baixado, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Se for a última página ou não houver dados, parar
                    if isinstance(data, dict):
                        result = data.get("result", [])
                        total_pages = data.get("totalPages", 1)
                        current_page = data.get("page", "1")
                        
                        logger.info(f"[main] Página {page}: {len(result) if isinstance(result, list) else 0} fundos, página {current_page} de {total_pages}")
                        
                        # Se chegou na última página, parar
                        if int(current_page) >= int(total_pages):
                            logger.info(f"[main] Última página ({current_page}) alcançada")
                            break
                        
                        # Se não há dados, também parar
                        if not isinstance(result, list) or len(result) == 0:
                            logger.info(f"[main] Página {page} sem dados, parando")
                            break
                            
                except Exception as e:
                    logger.warning(f"[main] Erro ao analisar página {page}: {e}")
                    # Continuar mesmo com erro de análise
            else:
                logger.warning(f"[main] Página {page} processada mas arquivo não encontrado")
                break
        else:
            logger.warning(f"[main] Falha ao baixar página {page}")
            # Para páginas > 1, não é necessariamente um erro crítico
            if page == 1:
                break  # Se a primeira página falhar, é erro crítico
            else:
                break  # Se páginas subsequentes falharem, pode ser que não existam

    # 5) Verificar resultado
    duracao_total = round(time.time() - start_time, 3)
    
    if not all_json_files:
        fallback = {
            "status": "FALHA",
            "total_jsons": 0,
            "data_referencia": data_ref.strftime("%Y-%m-%d"),
            "output_path": str(pasta_jsons),
            "erros": ["Nenhum JSON gerado."],
            "duracao_segundos": duracao_total
        }
        print(json.dumps(fallback, ensure_ascii=False))
        sys.exit(1)

    # Sucesso
    total_jsons = len(all_json_files)
    logger.info(f"[main] Processamento concluído com sucesso: {total_jsons} arquivos JSON")

    metrics = {
        "status": "SUCESSO",
        "total_jsons": total_jsons,
        "data_referencia": data_ref.strftime("%Y-%m-%d"),
        "output_path": str(pasta_jsons.resolve()),
        "erros": [],
        "duracao_segundos": duracao_total
    }
    print(json.dumps(metrics, ensure_ascii=False))
    sys.exit(0)

if __name__ == "__main__":
    main()