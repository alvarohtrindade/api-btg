#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para download de relatórios de Caixa Extrato do BTG Pactual Asset Management.

Autor: Álvaro - Equipe Data Analytics - Catalise Investimentos
Data: 07/06/2025
Versão: 1.2.0 (Detecção inteligente de dados inexistentes)
"""

import requests
import datetime
import os
import argparse
import traceback
import time
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

# Ajusta o sys.path para módulos utilitários
ROOT_PATH = Path(__file__).resolve().parents[2]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from dotenv import load_dotenv
from utils.date_utils import get_business_day
from utils.logging_utils import Log, LogLevel
from utils.backoff_utils import with_backoff_jitter

# Configuração de logs centralizada e sincronizada
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)

# Nome do arquivo de log baseado no script e data atual
log_filename = f"api_faas_extrato_{datetime.datetime.now().strftime('%Y%m%d')}.log"
log_file_path = LOGS_DIR / log_filename
Log.set_log_file(str(log_file_path), append=True, max_size_mb=5.0)

logger = Log.get_logger(__name__)

# Carrega variáveis de ambiente
env_path = ROOT_PATH / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

AUTH_URL       = os.getenv("AUTH_URL")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SCOPE          = os.getenv("SCOPE_EXTRATO")
GRANT_TYPE     = os.getenv("GRANT_TYPE", "client_credentials")
EXTRATO_URL    = os.getenv("EXTRATO_URL", "https://funds.btgpactual.com/reports/Cash/FundAccountStatement")
TICKET_URL     = os.getenv("TICKET_URL")
DOWNLOAD_PATH  = os.getenv("BTG_EXTRATO_PATH")

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
    logger.info("Obtendo token de autenticação para Caixa Extrato...")
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
def request_extrato_ticket(token: str, date_str: str) -> str:
    """Solicita ticket POST /reports/Cash/FundAccountStatement."""
    url = EXTRATO_URL
    headers = {
        "Accept": "application/json",
        "X-SecureConnect-Token": token,
        "Content-Type": "application/json-patch+json"
    }
    payload = {
        "contract": {
            "startDate": f"{date_str}T00:00:00.000Z",
            "endDate": f"{date_str}T23:59:59.000Z",
            "fundName": ""
        },
        "pageSize": 100,
        "webhookEndpoint": ""
    }
    logger.info(f"Solicitando ticket POST {url} para {date_str}")
    logger.debug(f"Payload da requisição: {json.dumps(payload, indent=2)}")
    
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

def check_data_availability(date_str: str) -> tuple[bool, str]:
    """
    Verifica se a data solicitada é válida para extração.
    Retorna (is_valid, warning_message)
    """
    try:
        data_solicitada = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        hoje = datetime.date.today()
        
        warnings = []
        
        # Verifica se a data não é muito antiga (mais de 2 anos)
        dois_anos_atras = hoje - datetime.timedelta(days=730)
        if data_solicitada < dois_anos_atras:
            warnings.append(f"Data {date_str} é muito antiga (>2 anos). Dados provavelmente não disponíveis.")
            return False, "; ".join(warnings)
        
        # Verifica se a data não é futura
        if data_solicitada > hoje:
            warnings.append(f"Data {date_str} é futura. Não é possível extrair dados futuros.")
            return False, "; ".join(warnings)
        
        # Verifica se é muito recente (últimos 2 dias úteis - dados podem não estar consolidados)
        if data_solicitada > hoje - datetime.timedelta(days=3):
            warnings.append(f"Data {date_str} é muito recente. Dados podem não estar consolidados.")
        
        # Verifica se é fim de semana
        if data_solicitada.weekday() >= 5:  # 5=sábado, 6=domingo
            warnings.append(f"Data {date_str} é fim de semana. Normalmente não há movimentação financeira.")
        
        # Verifica se é muito antiga (mais de 6 meses) - aviso
        seis_meses_atras = hoje - datetime.timedelta(days=180)
        if data_solicitada < seis_meses_atras:
            warnings.append(f"Data {date_str} é antiga (>6 meses). Dados podem ter sido arquivados.")
        
        warning_msg = "; ".join(warnings) if warnings else ""
        return True, warning_msg
    except ValueError:
        return False, f"Data {date_str} tem formato inválido. Use YYYY-MM-DD"

def test_recent_data_availability(token: str) -> bool:
    """
    Testa se consegue obter dados de uma data recente para validar que o endpoint funciona.
    """
    try:
        # Testa com ontem (mais provável de ter dados)
        ontem = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Testando disponibilidade de dados com data recente: {ontem}")
        
        # Solicita ticket para ontem
        test_ticket = request_extrato_ticket(token, ontem)
        
        # Faz uma única tentativa rápida
        url = f"{TICKET_URL}?ticketId={test_ticket}&pageNumber=1"
        headers = {
            "Accept": "application/json",
            "X-SecureConnect-Token": token
        }
        
        time.sleep(2)  # Aguarda um pouco
        resp = requests.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                result = data.get("result")
                if isinstance(result, list) and len(result) > 0:
                    logger.info(f"✅ Endpoint funciona - encontrados {len(result)} registros para {ontem}")
                    return True
                elif result != "Aguardando processamento":
                    logger.info(f"✅ Endpoint funciona - resposta válida para {ontem}: {result}")
                    return True
                else:
                    logger.info(f"⚠️ Endpoint responde mas dados estão processando para {ontem}")
                    return True
            except json.JSONDecodeError:
                logger.warning("⚠️ Resposta não é JSON válido")
                return False
        else:
            logger.warning(f"⚠️ Status HTTP {resp.status_code} para data de teste")
            return False
            
    except Exception as e:
        logger.warning(f"⚠️ Erro ao testar disponibilidade: {e}")
        return False

def download_extrato_json_intelligent(token: str, ticket: str, output_dir: Path, page_number: int = 1, date_str: str = "") -> bool:
    """
    Versão inteligente que detecta quando dados realmente não existem vs. quando estão processando.
    """
    url = f"{TICKET_URL}?ticketId={ticket}&pageNumber={page_number}"
    
    # Configuração adaptativa baseada na idade da data
    try:
        data_solicitada = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        hoje = datetime.date.today()
        dias_atras = (hoje - data_solicitada).days
        
        # Para dados antigos: timeout menor (provavelmente não existem)
        # Para dados recentes: timeout maior (podem estar processando)
        if dias_atras > 30:  # Mais de 1 mês
            max_attempts = 4
            wait_time = 10
            total_timeout = 60  # 1 minuto total
        elif dias_atras > 7:  # Mais de 1 semana
            max_attempts = 5
            wait_time = 15
            total_timeout = 90  # 1.5 minutos total
        else:  # Dados recentes
            max_attempts = 6
            wait_time = 20
            total_timeout = 120  # 2 minutos total
            
    except ValueError:
        # Fallback se a data for inválida
        max_attempts = 4
        wait_time = 15
        total_timeout = 60
    
    logger.info(f"Configuração adaptativa: {max_attempts} tentativas, {wait_time}s intervalo, {total_timeout}s timeout total")
    
    start_time = time.time()
    consecutive_processing = 0  # Contador de "Aguardando processamento" consecutivos
    
    for attempt in range(1, max_attempts + 1):
        try:
            # Verifica se já passou do timeout total
            elapsed = time.time() - start_time
            if elapsed > total_timeout:
                logger.warning(f"Timeout total de {total_timeout}s atingido")
                break
                
            logger.info(f"Tentativa {attempt}/{max_attempts} - Consultando API... (elapsed: {elapsed:.1f}s)")
            
            # Renovar token a cada tentativa
            try:
                fresh_token = get_token()
            except Exception as e:
                logger.error(f"Erro ao renovar token na tentativa {attempt}: {e}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue
                else:
                    return False
            
            headers = {
                "Accept": "application/json",
                "X-SecureConnect-Token": fresh_token
            }
            
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code == 401:
                logger.warning(f"Token expirado (401) na tentativa {attempt}")
                if attempt < max_attempts:
                    time.sleep(2)
                    continue
                else:
                    return False
            
            if resp.status_code != 200:
                logger.warning(f"Status HTTP {resp.status_code} na tentativa {attempt}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue
                else:
                    return False
            
            content_type = resp.headers.get("Content-Type", "").lower()
            
            if 'application/json' in content_type:
                try:
                    data = resp.json()
                    result = data.get("result")
                    total_pages = data.get("totalPages")
                    current_page = data.get("page")
                    
                    logger.info(f"Resposta: result='{result}', totalPages={total_pages}, page={current_page}")
                    
                    # LÓGICA INTELIGENTE: Detectar quando dados não existem
                    if result == "Processando" or result == "Aguardando processamento":
                        consecutive_processing += 1
                        
                        # Se já temos muitas tentativas consecutivas de "processando", provavelmente não há dados
                        if consecutive_processing >= 3:
                            logger.warning(f"🔍 Detectado padrão: {consecutive_processing} tentativas consecutivas de 'processamento'")
                            
                            # Para dados antigos, assumir que não há dados
                            if dias_atras > 30:
                                logger.info(f"📊 Conclusão: Data antiga ({dias_atras} dias) + processamento contínuo = SEM DADOS")
                                # Salva um arquivo indicando que não há dados
                                output_dir.mkdir(parents=True, exist_ok=True)
                                filename = output_dir / f"{ticket}_p{page_number}_no_data.json"
                                no_data_response = {
                                    "result": [],
                                    "totalPages": 0,
                                    "page": 1,
                                    "message": f"Sem dados disponíveis para {date_str}",
                                    "detection_reason": f"Data antiga ({dias_atras} dias) com processamento contínuo",
                                    "attempts": attempt
                                }
                                with open(filename, "w", encoding="utf-8") as f:
                                    json.dump(no_data_response, f, ensure_ascii=False, indent=2)
                                logger.info(f"✅ Arquivo 'sem dados' criado: {filename}")
                                return True
                        
                        logger.info(f"Relatório em processamento (tentativa {attempt}/{max_attempts}, consecutivas: {consecutive_processing}). Aguardando {wait_time}s.")
                        
                        if attempt < max_attempts:
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Timeout final: relatório ainda processando após {max_attempts} tentativas")
                            return False
                    
                    # Reset contador se recebemos resposta diferente
                    consecutive_processing = 0
                    
                    # Verificar se há erro específico
                    if isinstance(result, str) and ("erro" in result.lower() or "error" in result.lower()):
                        logger.error(f"Erro reportado pela API: {result}")
                        return False
                    
                    # Verificar se há dados válidos
                    if isinstance(result, list):
                        output_dir.mkdir(parents=True, exist_ok=True)
                        filename = output_dir / f"{ticket}_p{page_number}.json"
                        
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        
                        if len(result) > 0:
                            logger.info(f"✅ Dados válidos encontrados: {filename} ({len(result)} registros)")
                            return True
                        else:
                            logger.info(f"✅ Resposta válida mas sem registros: {filename}")
                            return True
                    
                    # Para outros tipos de resposta, salvar e considerar sucesso
                    output_dir.mkdir(parents=True, exist_ok=True)
                    filename = output_dir / f"{ticket}_p{page_number}.json"
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    logger.info(f"✅ Resposta salva (estrutura inesperada): {filename}")
                    return True
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Erro ao decodificar JSON na tentativa {attempt}: {e}")
                    if attempt < max_attempts:
                        time.sleep(wait_time)
                        continue
                    else:
                        return False
            else:
                logger.error(f"Tipo de conteúdo inesperado: {content_type}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue
                else:
                    return False
                    
        except requests.Timeout as e:
            logger.warning(f"Timeout na tentativa {attempt}: {e}")
            if attempt < max_attempts:
                time.sleep(wait_time)
                continue
            else:
                return False
                
        except Exception as e:
            logger.error(f"Erro inesperado na tentativa {attempt}: {e}")
            if attempt < max_attempts:
                time.sleep(wait_time)
                continue
            else:
                return False

    logger.error(f"❌ Falha após {max_attempts} tentativas")
    return False

def main(date_str: str, base_output: Path) -> int:
    """
    Fluxo principal com detecção inteligente de disponibilidade de dados.
    """
    logger.info(f"=== INICIANDO EXTRAÇÃO DE CAIXA EXTRATO PARA {date_str} ===")
    output_dir = base_output / "extrato" / date_str

    # Validação prévia da data
    is_valid, warning_msg = check_data_availability(date_str)
    if not is_valid:
        logger.error(f"❌ Data {date_str} não é válida: {warning_msg}")
        metrics = {
            "status": "FALHA",
            "total_arquivos": 0,
            "tamanho_total": 0,
            "duracao_segundos": 0,
            "erros": [f"Data {date_str} inválida: {warning_msg}"]
        }
        print(json.dumps(metrics, ensure_ascii=False))
        return 0
    
    if warning_msg:
        logger.warning(f"⚠️ Aviso para {date_str}: {warning_msg}")

    start_time = time.time()
    
    try:
        # Obter token
        logger.info("🔑 Etapa 1: Obtendo token de autenticação...")
        token = get_token()
        
        # Teste de conectividade (opcional - só para dados antigos)
        try:
            data_solicitada = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            hoje = datetime.date.today()
            dias_atras = (hoje - data_solicitada).days
            
            if dias_atras > 30:  # Para dados antigos, testa conectividade primeiro
                logger.info("🧪 Etapa 1.5: Testando conectividade do endpoint (data antiga)...")
                if not test_recent_data_availability(token):
                    logger.warning("⚠️ Endpoint pode estar com problemas ou sem dados recentes")
                    
        except ValueError:
            pass
        
        # Solicitar ticket
        logger.info("🎫 Etapa 2: Solicitando ticket de processamento...")
        ticket = request_extrato_ticket(token, date_str)
        
        # Aguardar processamento inicial
        logger.info("⏳ Etapa 3: Aguardando processamento inicial (3s)...")
        time.sleep(3)
        
        # Download inteligente
        logger.info("📥 Etapa 4: Iniciando download inteligente de dados...")
        all_json_files = []
        max_pages = 2  # Reduzido ainda mais para evitar requisições desnecessárias
        
        for page in range(1, max_pages + 1):
            logger.info(f"📄 Processando página {page}/{max_pages}")
            success = download_extrato_json_intelligent(token, ticket, output_dir, page, date_str)
            
            if success:
                # Procura por qualquer arquivo gerado (dados ou no_data)
                arquivos_gerados = list(output_dir.glob(f"{ticket}_p{page}*.json"))
                if arquivos_gerados:
                    arquivo_baixado = arquivos_gerados[0]
                    all_json_files.append(str(arquivo_baixado))
                    logger.info(f"✅ Página {page} processada: {arquivo_baixado.name}")
                    
                    # Analisar conteúdo para decisão de continuar
                    try:
                        with open(arquivo_baixado, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Se é um arquivo de "sem dados", parar
                        if "no_data" in arquivo_baixado.name or data.get("message") == f"Sem dados disponíveis para {date_str}":
                            logger.info("🛑 Detectado arquivo 'sem dados' - parando busca")
                            break
                        
                        if isinstance(data, dict):
                            result = data.get("result", [])
                            total_pages = data.get("totalPages")
                            current_page = data.get("page")
                            
                            if isinstance(result, list):
                                logger.info(f"📊 Página {page}: {len(result)} registros")
                            
                            if total_pages and current_page:
                                if int(current_page) >= int(total_pages):
                                    logger.info(f"🏁 Última página ({current_page}) alcançada")
                                    break
                            
                            # Se primeira página está vazia, provavelmente não há mais dados
                            if page == 1 and isinstance(result, list) and len(result) == 0:
                                logger.info("🛑 Primeira página vazia - parando busca")
                                break
                                
                    except Exception as e:
                        logger.warning(f"⚠️ Erro ao analisar página {page}: {e}")
                else:
                    logger.warning(f"❌ Página {page} processada mas nenhum arquivo foi criado")
                    break
            else:
                logger.warning(f"❌ Falha ao processar página {page}")
                if page == 1:
                    logger.error("💥 Falha na primeira página - encerrando extração")
                    break
                else:
                    logger.info("ℹ️ Falha em página adicional - assumindo fim dos dados")
                    break
        
        total_files = len(all_json_files)
        duracao = round(time.time() - start_time, 3)

        # Calcular tamanho total dos arquivos
        tamanho_bytes = 0
        for file_path in all_json_files:
            try:
                tamanho_bytes += Path(file_path).stat().st_size
            except:
                pass
        
        # Log final detalhado
        logger.info(f"🎯 === EXTRAÇÃO CONCLUÍDA ===")
        logger.info(f"📅 Data processada: {date_str}")
        logger.info(f"📁 Total de arquivos: {total_files}")
        logger.info(f"💾 Tamanho total: {tamanho_bytes} bytes ({tamanho_bytes/1024:.1f} KB)")
        logger.info(f"⏱️ Duração: {duracao}s")
        logger.info(f"📂 Arquivos salvos em: {output_dir}")
        
        if total_files > 0:
            logger.info("✅ Status: SUCESSO - Dados extraídos ou confirmada ausência de dados")
        else:
            logger.warning("⚠️ Status: SEM DADOS - Nenhum arquivo foi gerado")
        
        # Métricas de sucesso
        metrics = {
            "status": "SUCESSO" if total_files > 0 else "SEM_DADOS",
            "total_arquivos": total_files,
            "tamanho_total": tamanho_bytes,
            "duracao_segundos": duracao,
            "erros": []
        }
        
        print(json.dumps(metrics, ensure_ascii=False))
        return total_files

    except Exception as e:
        duracao = round(time.time() - start_time, 3)
        error_msg = str(e)
        
        logger.error(f"💥 === ERRO DURANTE EXTRAÇÃO ===")
        logger.error(f"📅 Data: {date_str}")
        logger.error(f"❌ Erro: {error_msg}")
        logger.error(f"📋 Traceback completo:")
        logger.error(traceback.format_exc())
        
        # Métricas de falha
        metrics = {
            "status": "FALHA",
            "total_arquivos": 0,
            "tamanho_total": 0,
            "duracao_segundos": duracao,
            "erros": [error_msg]
        }
        
        print(json.dumps(metrics, ensure_ascii=False))
        return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Caixa Extrato BTG - Versão Inteligente')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--n-days', type=int, help='Dias úteis atrás')
    group.add_argument('--date', type=str, help='Data no formato YYYY-MM-DD')
    parser.add_argument(
        '--output-dir-base',
        type=str,
        default=os.getenv('OUTPUT_DIR_BASE', 'output'),
        help='Diretório base onde ficarão as pastas "extrato"'
    )
    args = parser.parse_args()

    # Log de inicialização
    logger.info("🚀 === SCRIPT INICIADO ===")
    logger.info(f"⚙️ Argumentos recebidos: {vars(args)}")
    logger.info(f"📝 Arquivo de log: {log_file_path}")

    try:
        if args.n_days is not None:
            d = get_business_day(n_days=args.n_days).strftime('%Y-%m-%d')
            logger.info(f"📅 Data calculada com --n-days {args.n_days}: {d}")
        elif args.date:
            datetime.datetime.strptime(args.date, '%Y-%m-%d')
            d = args.date
            logger.info(f"📅 Data especificada: {d}")
        else:
            # Pega dia útil anterior
            d = get_business_day(n_days=1).strftime('%Y-%m-%d')
            logger.info(f"📅 Data padrão (1 dia útil atrás): {d}")
    except Exception as e:
        error_msg = f"Formato de data inválido. Use YYYY-MM-DD ou --n-days. Erro: {str(e)}"
        logger.error(f"❌ {error_msg}")
        error_metrics = {
            "status": "FALHA",
            "total_arquivos": 0,
            "tamanho_total": 0,
            "duracao_segundos": 0,
            "erros": [error_msg]
        }
        print(json.dumps(error_metrics, ensure_ascii=False))
        sys.exit(1)

    base_out = Path(args.output_dir_base)
    logger.info(f"📂 Diretório base de saída: {base_out}")
    
    result = main(d, base_out)
    
    logger.info(f"🏁 === SCRIPT FINALIZADO ===")
    logger.info(f"📊 Resultado: {result} arquivo(s) extraído(s)")
    
    sys.exit(0 if result >= 0 else 1)