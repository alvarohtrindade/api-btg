#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquestrador BTG ETL (Carteira + Rentabilidade + Extrato)

Autor: Álvaro – Equipe Data Analytics – Catalise Investimentos
Data: 01/06/2025
Versão: 1.4.0 (Corrigido processamento de range de datas e logs sincronizados)
"""

import os
import argparse
import subprocess
import datetime
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Any

# Ajusta o sys.path para importar módulos da raiz do projeto
ROOT_PATH = Path(__file__).resolve().parents[2]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from utils.logging_utils import Log, LogLevel
from utils.notification_manager import NotificationManager, NotificationType, TemplateNotification
from utils.date_utils import get_business_day

# Configuração de logs centralizada e sincronizada
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)

# Nome do arquivo de log baseado no script e data atual
hoje_str = datetime.datetime.now().strftime('%Y%m%d')
log_filename = f"orquestrador_btg_{hoje_str}.log"
log_file_path = LOGS_DIR / log_filename
Log.set_log_file(str(log_file_path), append=True, max_size_mb=10.0)

logger = Log.get_logger(__name__)

# Carrega .env
dotenv_path = ROOT_PATH / '.env'
load_dotenv(dotenv_path=dotenv_path)

# Configuração de e-mail
RECEIVER_EMAIL = [
    e.strip() for e in os.getenv("RECEIVER_EMAIL", "").split(",") if e.strip()
]

notification_manager = NotificationManager()
EMAIL_TEMPLATES_DIR = ROOT_PATH / "configs" / "templates"
EMAIL_TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

def generate_business_days_range(start_date: str, end_date: str) -> List[str]:
    """
    Gera uma lista de dias úteis entre duas datas (inclusivo).
    Versão otimizada que valida datas e limita o range.
    """
    try:
        # Validar formato das datas
        start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Validar ordem das datas
        if start_dt > end_dt:
            raise ValueError(f"Data inicial ({start_date}) é posterior à data final ({end_date})")
        
        # Calcular número de dias
        delta_days = (end_dt - start_dt).days + 1
        
        if delta_days > 90:
            logger.warning(f"Range muito amplo: {delta_days} dias. Limitando a 90 dias para evitar sobrecarga.")
            end_dt = start_dt + datetime.timedelta(days=89)
            end_date = end_dt.strftime('%Y-%m-%d')
        
        logger.info(f"Gerando dias úteis entre {start_date} e {end_date} ({delta_days} dias)")
        
        try:
            from utils.mysql_connector_utils import MySQLConnector
            
            connector = MySQLConnector.from_env()
            
            query = '''
            SELECT DtReferencia
            FROM vw_calendario
            WHERE DtReferencia BETWEEN %s AND %s
              AND Feriado = 0
              AND FimSemana = 0
            ORDER BY DtReferencia
            '''
            
            results = connector.execute_query(query, (start_date, end_date))
            business_days = [row['DtReferencia'].strftime('%Y-%m-%d') for row in results]
            
            connector.close()
            
            logger.info(f"Encontrados {len(business_days)} dias úteis no período")
            return business_days
            
        except Exception as e:
            logger.warning(f"Erro ao consultar banco para dias úteis: {e}")
            logger.info("Usando fallback com pandas para gerar dias úteis")
            
            import pandas as pd
            dates = pd.date_range(start=start_date, end=end_date, freq='B')
            business_days = [date.strftime('%Y-%m-%d') for date in dates]
            
            logger.info(f"Fallback gerou {len(business_days)} dias úteis")
            return business_days
        
    except ValueError as e:
        logger.error(f"Erro ao gerar range de datas: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao gerar dias úteis: {e}")
        raise

def run_command(command: List[str], step_name: str, log_output: bool = True) -> Tuple[int, str]:
    """Executa um comando externo via subprocess, capturando stdout+stderr."""
    logger.info(f"=== EXECUTANDO: {step_name} ===")
    logger.info(f"Comando: {' '.join(command)}")
    
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    out_lines = []
    for ln in proc.stdout:
        line_stripped = ln.strip()
        if line_stripped:  # Só loga linhas não vazias
            if log_output:
                logger.info(f"[{step_name}] {line_stripped}")
            out_lines.append(ln)
    
    proc.wait()
    full_output = "".join(out_lines)
    
    if proc.returncode != 0:
        logger.error(f"=== FALHA: {step_name} (código {proc.returncode}) ===")
    else:
        logger.info(f"=== SUCESSO: {step_name} ===")
    
    return proc.returncode, full_output

def parse_metrics_from_output(output: str) -> Dict[str, Any]:
    """
    Busca JSON de métricas na saída de forma mais robusta.
    """
    if not output or not output.strip():
        logger.warning("Saída vazia para parsing de métricas")
        return {}
    
    # Procura por linhas que começam com identificadores conhecidos
    for ln in output.splitlines():
        stripped = ln.strip()
        if (stripped.startswith("Métricas de Extração:") or 
            stripped.startswith("Métricas de Processamento:") or 
            stripped.startswith("Métricas de Inserção")):
            try:
                json_part = stripped.split(":", 1)[1].strip()
                parsed = json.loads(json_part)
                logger.debug(f"Métricas extraídas via identificador: {list(parsed.keys())}")
                return parsed
            except (json.JSONDecodeError, IndexError) as e:
                logger.warning(f"Erro ao parsear métrica com identificador: {e}")
                continue

    # Procura por JSON puro nas últimas linhas (mais provável)
    lines = output.strip().splitlines()
    for ln in reversed(lines[-10:]):  # Verifica últimas 10 linhas
        stripped = ln.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                parsed = json.loads(stripped)
                # Verifica se tem estrutura de métricas conhecidas
                expected_keys = [
                    "total_arquivos_processados", "total_fundos", "detalhamento",
                    "status", "total_jsons", "total_registros_inseridos",
                    "detalhamento_por_fundo", "total_fundos_unicos", "total_arquivos",
                    "tamanho_total", "duracao_segundos"
                ]
                if any(key in parsed for key in expected_keys):
                    logger.debug(f"Métricas encontradas via JSON: {list(parsed.keys())}")
                    return parsed
            except json.JSONDecodeError:
                continue

    logger.warning("Nenhuma métrica válida encontrada na saída")
    logger.debug(f"Últimas 5 linhas da saída: {lines[-5:] if lines else 'N/A'}")
    return {}

def build_processing_rows(raw_detalhamento: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transforma detalhamento em formato compatível com template."""
    rows: List[Dict[str, Any]] = []
    for item in raw_detalhamento:
        status_full = str(item.get("Status", "")).upper()
        if "SUCESSO" in status_full or status_full == "OK":
            status_text = "success"
        elif "IGNORADO" in status_full or "SEM DADOS" in status_full:
            status_text = "skipped"
        else:
            status_text = "failure"

        if status_text == "skipped":
            total = inserted = duration = "-"
        else:
            total = item.get("Total Linhas", 0)
            inserted = item.get("Inseridos", 0)
            duration = item.get("Duração (s)", 0)

        rows.append({
            "fundo": item.get("Arquivo", ""),
            "date": item.get("Data Processo", ""),
            "total": total,
            "inserted": inserted,
            "duration": duration,
            "status_text": status_text
        })
    return rows

def send_error_email(data_ref: str, error_message: str, step: str):
    """Envia email de erro específico."""
    tmpl = {
        "status": "FALHA",
        "data_referencia": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "date_range": data_ref,
        "total_dates_processed": 1,
        "duracao_total": "0 s",
        "extracao_num_arquivos": 0,
        "extracao_tamanho_total": "0 MB",
        "extracao_duracao": "0 s",
        "processamento_total_arquivos": 0,
        "processamento_total_registros": 0,
        "processamento_duracao": "0 s",
        "processing_rows_carteira": [],
        "rentabilidade_total_arquivos": 0,
        "rentabilidade_total_registros": 0,
        "rentabilidade_total_fundos": 0,
        "rentabilidade_duracao": "0 s",
        "processing_rows_rent": [],
        "detalhamento_por_fundo_rent": [],
        "extrato_total_arquivos": 0,
        "extrato_total_registros": 0,
        "extrato_duracao": "0 s",
        "processing_rows_extrato": [],
        "execution_id": datetime.datetime.now().isoformat(),
        "log_path": str(log_file_path),
        "errors_section": f"<p><strong>Erro em {step}:</strong> {error_message}</p>",
        "skipped_files_section": "",
        "critical_funds_section": "",
        "missing_funds_section": ""
    }
    
    try:
        notification_manager.send_with_template(
            TemplateNotification(
                type=NotificationType.EMAIL,
                recipients=RECEIVER_EMAIL,
                subject=f"🚨 ERRO ETL BTG - {step} ({data_ref})",
                template_path=str(EMAIL_TEMPLATES_DIR / "btg_carteira_report.html"),
                context=tmpl,
                kwargs={"is_html": True}
            )
        )
        logger.info(f"Email de erro enviado para {step}")
    except Exception as e:
        logger.error(f"Falha ao enviar email de erro: {e}")

def process_single_date(data_ref: str, base: Path, args) -> Dict[str, Any]:
    """
    Processa uma única data para os endpoints selecionados.
    Versão otimizada com melhor tratamento de erros e logs.
    """
    logger.info(f"=== PROCESSANDO DATA: {data_ref} ===")
    logger.info(f"Endpoints selecionados: {args.endpoints}")
    logger.info(f"Skip insertion: {args.skip_insertion}")
    
    extracted_data_dir = base / "extracted" / data_ref
    
    # Estrutura de métricas para esta data
    date_metrics = {
        "data": data_ref,
        "extracao": {
            "status": "NÃO EXECUTADO", "num_arquivos": 0, "tamanho_bytes": 0, 
            "duracao_segundos": 0, "erros": []
        },
        "processamento": {
            "status": "NÃO EXECUTADO", "total_arquivos_processados": 0, 
            "total_registros_inseridos": 0, "duracao_segundos": 0, 
            "erros": [], "detalhamento": []
        },
        "rentabilidade": {
            "status": "NÃO EXECUTADO", "total_arquivos_processados": 0, 
            "total_registros_inseridos": 0, "total_fundos_unicos": 0, 
            "duracao_segundos": 0, "erros": [], "detalhamento": [], 
            "detalhamento_por_fundo": []
        },
        "extrato": {
            "status": "NÃO EXECUTADO", "total_arquivos_processados": 0, 
            "total_registros_inseridos": 0, "duracao_segundos": 0, 
            "erros": [], "detalhamento": []
        }
    }

    # ETAPA 1: EXTRAÇÃO de Carteira Diária
    if 'carteira' in args.endpoints:
        logger.info(f"=== INICIANDO EXTRAÇÃO DE CARTEIRA - {data_ref} ===")
        start_ext = datetime.datetime.now()
        cmd_ext = [
            sys.executable,
            str(Path(__file__).parent / "api_faas_portfolio.py"),
            "--date", data_ref,
            "--output-dir-base", str(base)
        ]
        code_ext, out_ext = run_command(cmd_ext, f"Extracao_Carteira_{data_ref}")
        end_ext = datetime.datetime.now()
        
        metrics_ext = parse_metrics_from_output(out_ext)
        
        # Calcula métricas reais da extração
        raw_dir = base / "raw"
        extracted_dir = base / "extracted" / data_ref
        
        arquivos_extraidos = []
        if raw_dir.exists():
            arquivos_extraidos = list(raw_dir.glob("*.zip")) + list(raw_dir.glob("*.xlsx"))
        if extracted_dir.exists():
            arquivos_extraidos.extend(list(extracted_dir.glob("*.xlsx")))
        
        num_files = len(arquivos_extraidos)
        total_bytes = sum(f.stat().st_size for f in arquivos_extraidos if f.exists())
        dur_ext = (end_ext - start_ext).total_seconds()

        date_metrics["extracao"].update({
            "status": "SUCESSO" if code_ext == 0 else "FALHA",
            "num_arquivos": num_files,
            "tamanho_bytes": total_bytes,
            "duracao_segundos": dur_ext,
            "erros": metrics_ext.get("erros", []) if code_ext != 0 else []
        })

        if code_ext != 0:
            logger.error(f"Falha na extração de carteira para {data_ref}")
            return date_metrics

        # ETAPA 2: INSERÇÃO no Banco de Dados (Carteira)
        if not args.skip_insertion:
            logger.info(f"=== INICIANDO PROCESSAMENTO DE CARTEIRA - {data_ref} ===")
            insert_script_path = ROOT_PATH / "backend" / "api_btg" / "insert_db" / "insert_carteira.py"
            if insert_script_path.exists():
                start_ins = datetime.datetime.now()
                insert_cmd = [
                    sys.executable,
                    str(insert_script_path),
                    "--date", data_ref,
                    "--input-dir", str(extracted_data_dir)
                ]
                code_ins, out_ins = run_command(insert_cmd, f"Insercao_Carteira_{data_ref}")
                end_ins = datetime.datetime.now()
                
                metrics_ins = parse_metrics_from_output(out_ins)
                dur_ins = (end_ins - start_ins).total_seconds()
                
                date_metrics["processamento"].update({
                    "status": "SUCESSO" if code_ins == 0 else "FALHA",
                    "total_arquivos_processados": metrics_ins.get("total_arquivos_processados", 0),
                    "total_registros_inseridos": metrics_ins.get("total_registros_inseridos", 0),
                    "duracao_segundos": dur_ins,
                    "erros": metrics_ins.get("erros", []),
                    "detalhamento": metrics_ins.get("detalhamento", [])
                })
            else:
                logger.warning(f"Script de inserção não encontrado: {insert_script_path}")
        else:
            logger.info("Inserção de carteira pulada (--skip-insertion)")

    # ETAPA 3: EXTRAÇÃO + INSERÇÃO de Rentabilidade
    if 'rentabilidade' in args.endpoints:
        logger.info(f"=== INICIANDO PROCESSAMENTO DE RENTABILIDADE - {data_ref} ===")
        
        # Extração de Rentabilidade
        start_rent_ext = datetime.datetime.now()
        rent_ext_script = ROOT_PATH / "backend" / "api_btg" / "api_faas_rentabilidade.py"
        rent_ext_cmd = [
            sys.executable,
            str(rent_ext_script),
            "--json-dir", str(base / "raw_rent" / data_ref),
            "--date", data_ref
        ]
        code_rent_ext, out_rent_ext = run_command(rent_ext_cmd, f"Extracao_Rentabilidade_{data_ref}")
        end_rent_ext = datetime.datetime.now()
        
        metrics_rent_ext = parse_metrics_from_output(out_rent_ext)
        dur_rent_ext = (end_rent_ext - start_rent_ext).total_seconds()

        pasta_jsons_rent = base / "raw_rent" / data_ref
        arquivos_json = list(pasta_jsons_rent.glob("*.json")) if pasta_jsons_rent.exists() else []
        total_arquivos_rent = len(arquivos_json)

        date_metrics["rentabilidade"].update({
            "status": "SUCESSO" if code_rent_ext == 0 else "FALHA",
            "total_arquivos_processados": total_arquivos_rent,
            "duracao_segundos": dur_rent_ext,
            "erros": metrics_rent_ext.get("erros", [])
        })

        # Inserção Rentabilidade
        if code_rent_ext == 0 and not args.skip_insertion and total_arquivos_rent > 0:
            rent_insert_script = ROOT_PATH / "backend" / "api_btg" / "insert_db" / "insert_rentabilidade.py"
            if rent_insert_script.exists():
                start_rent_ins = datetime.datetime.now()
                rent_ins_cmd = [
                    sys.executable,
                    str(rent_insert_script),
                    "--json-dir", str(pasta_jsons_rent),
                    "--auto"
                ]
                code_rent_ins, out_rent_ins = run_command(rent_ins_cmd, f"Insercao_Rentabilidade_{data_ref}")
                end_rent_ins = datetime.datetime.now()
                
                metrics_rent_ins = parse_metrics_from_output(out_rent_ins)
                dur_rent_ins = (end_rent_ins - start_rent_ins).total_seconds()
                
                date_metrics["rentabilidade"].update({
                    "status": "SUCESSO" if code_rent_ins == 0 else "FALHA",
                    "total_registros_inseridos": metrics_rent_ins.get("total_registros_inseridos", 0),
                    "total_fundos_unicos": metrics_rent_ins.get("total_fundos_unicos", 0),
                    "duracao_segundos": dur_rent_ins,
                    "detalhamento": metrics_rent_ins.get("detalhamento", []),
                    "detalhamento_por_fundo": metrics_rent_ins.get("detalhamento_por_fundo", [])
                })
            else:
                logger.warning(f"Script de inserção rentabilidade não encontrado: {rent_insert_script}")
        elif args.skip_insertion:
            logger.info("Inserção de rentabilidade pulada (--skip-insertion)")

    # ETAPA 4: EXTRAÇÃO + INSERÇÃO de Extrato
    if 'extrato' in args.endpoints:
        logger.info(f"=== INICIANDO PROCESSAMENTO DE EXTRATO - {data_ref} ===")
        
        # Extração de Extrato
        start_extrato_ext = datetime.datetime.now()
        extrato_ext_script = ROOT_PATH / "backend" / "api_btg" / "api_faas_extrato.py"
        extrato_ext_cmd = [
            sys.executable,
            str(extrato_ext_script),
            "--date", data_ref,
            "--output-dir-base", str(base)
        ]
        code_extrato_ext, out_extrato_ext = run_command(extrato_ext_cmd, f"Extracao_Extrato_{data_ref}")
        end_extrato_ext = datetime.datetime.now()
        
        metrics_extrato_ext = parse_metrics_from_output(out_extrato_ext)
        dur_extrato_ext = (end_extrato_ext - start_extrato_ext).total_seconds()

        pasta_jsons_extrato = base / "extrato" / data_ref
        arquivos_json_extrato = list(pasta_jsons_extrato.glob("*.json")) if pasta_jsons_extrato.exists() else []
        total_arquivos_extrato = len(arquivos_json_extrato)

        date_metrics["extrato"].update({
            "status": "SUCESSO" if code_extrato_ext == 0 else "FALHA",
            "total_arquivos_processados": total_arquivos_extrato,
            "duracao_segundos": dur_extrato_ext,
            "erros": metrics_extrato_ext.get("erros", [])
        })

        # Inserção Extrato
        if code_extrato_ext == 0 and not args.skip_insertion and total_arquivos_extrato > 0:
            extrato_insert_script = ROOT_PATH / "backend" / "api_btg" / "insert_db" / "insert_extrato.py"
            if extrato_insert_script.exists():
                start_extrato_ins = datetime.datetime.now()
                extrato_ins_cmd = [
                    sys.executable,
                    str(extrato_insert_script),
                    "--json-dir", str(pasta_jsons_extrato),
                    "--auto"
                ]
                code_extrato_ins, out_extrato_ins = run_command(extrato_ins_cmd, f"Insercao_Extrato_{data_ref}")
                end_extrato_ins = datetime.datetime.now()
                
                metrics_extrato_ins = parse_metrics_from_output(out_extrato_ins)
                dur_extrato_ins = (end_extrato_ins - start_extrato_ins).total_seconds()
                
                date_metrics["extrato"].update({
                    "status": "SUCESSO" if code_extrato_ins == 0 else "FALHA",
                    "total_registros_inseridos": metrics_extrato_ins.get("total_registros_inseridos", 0),
                    "duracao_segundos": dur_extrato_ins,
                    "detalhamento": metrics_extrato_ins.get("detalhamento", [])
                })
            else:
                logger.warning(f"Script de inserção extrato não encontrado: {extrato_insert_script}")
        elif args.skip_insertion:
            logger.info("Inserção de extrato pulada (--skip-insertion)")

    logger.info(f"=== PROCESSAMENTO CONCLUÍDO PARA {data_ref} ===")
    return date_metrics

def main():
    parser = argparse.ArgumentParser(description='Orquestrador ETL BTG (Carteira + Rentabilidade + Extrato)')
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument('--n-days', type=int, help='Dias úteis atrás')
    grp.add_argument('--date', type=str, help='Data YYYY-MM-DD')
    grp.add_argument('--date-range', type=str, help='Intervalo de datas YYYY-MM-DD:YYYY-MM-DD')
    parser.add_argument(
        '--output-dir-base',
        default=str(ROOT_PATH / "data" / "btg"),
        help='Diretório base onde ficarão as pastas "raw" e "extracted"'
    )
    parser.add_argument('--skip-insertion', action='store_true', help='Pular inserção no banco')
    parser.add_argument('--endpoints', nargs='+', choices=['carteira', 'rentabilidade', 'extrato'], 
                       default=['carteira', 'rentabilidade', 'extrato'],
                       help='Endpoints a serem processados')
    args = parser.parse_args()

    logger.info("=== ORQUESTRADOR BTG INICIADO ===")
    logger.info(f"Argumentos: {vars(args)}")
    logger.info(f"Arquivo de log: {log_file_path}")

    # Determina as datas a serem processadas
    try:
        if args.date_range:
            start_date, end_date = args.date_range.split(':')
            datetime.datetime.strptime(start_date, '%Y-%m-%d')
            datetime.datetime.strptime(end_date, '%Y-%m-%d')
            dates_to_process = generate_business_days_range(start_date, end_date)
            logger.info(f"Processamento em lote: {len(dates_to_process)} dias úteis entre {start_date} e {end_date}")
            
            if len(dates_to_process) > 50:
                logger.warning(f"Processamento de {len(dates_to_process)} datas pode demorar muito. Considere dividir em lotes menores.")
                
        elif args.n_days is not None:
            data_ref = get_business_day(n_days=args.n_days).strftime('%Y-%m-%d')
            dates_to_process = [data_ref]
            logger.info(f"Processamento único: {args.n_days} dias úteis atrás = {data_ref}")
        elif args.date:
            datetime.datetime.strptime(args.date, '%Y-%m-%d')
            dates_to_process = [args.date]
            logger.info(f"Processamento único: data específica = {args.date}")
        else:
            data_ref = get_business_day().strftime('%Y-%m-%d')
            dates_to_process = [data_ref]
            logger.info(f"Processamento padrão: dia útil anterior = {data_ref}")
            
        logger.info(f"Datas a processar: {dates_to_process}")
        logger.info(f"Endpoints selecionados: {args.endpoints}")
    except Exception as e:
        error_msg = f"Erro ao determinar datas: {e}"
        logger.error(error_msg)
        send_error_email("N/A", error_msg, "Validação de Data")
        sys.exit(1)

    base = Path(args.output_dir_base)
    logger.info(f"Diretório base: {base}")
    
    # Métricas consolidadas de todas as datas
    all_dates_metrics = []
    consolidated_metrics = {
        "extracao": {"status": "SUCESSO", "num_arquivos": 0, "tamanho_bytes": 0, "duracao_segundos": 0, "erros": []},
        "processamento": {"status": "SUCESSO", "total_arquivos_processados": 0, "total_registros_inseridos": 0, 
                         "duracao_segundos": 0, "erros": [], "detalhamento": []},
        "rentabilidade": {"status": "SUCESSO", "total_arquivos_processados": 0, "total_registros_inseridos": 0, 
                         "total_fundos_unicos": 0, "duracao_segundos": 0, "erros": [], 
                         "detalhamento": [], "detalhamento_por_fundo": []},
        "extrato": {"status": "SUCESSO", "total_arquivos_processados": 0, "total_registros_inseridos": 0, 
                   "duracao_segundos": 0, "erros": [], "detalhamento": []}
    }

    # Tempo de início global
    start_global = datetime.datetime.now()

    # Processa cada data
    for i, data_ref in enumerate(dates_to_process, 1):
        try:
            logger.info(f"=== PROCESSANDO DATA {i}/{len(dates_to_process)}: {data_ref} ===")
            date_metrics = process_single_date(data_ref, base, args)
            all_dates_metrics.append(date_metrics)
            
            # Consolida métricas apenas dos endpoints processados
            for endpoint in args.endpoints:
                if endpoint == "carteira":
                    # Carteira usa "extracao" e "processamento"
                    for key in ["extracao", "processamento"]:
                        if key in date_metrics:
                            date_endpoint_metrics = date_metrics[key]
                            for metric_key in ["num_arquivos", "tamanho_bytes", "duracao_segundos", 
                                             "total_arquivos_processados", "total_registros_inseridos"]:
                                if metric_key in date_endpoint_metrics and date_endpoint_metrics[metric_key]:
                                    consolidated_metrics[key][metric_key] = consolidated_metrics[key].get(metric_key, 0) + date_endpoint_metrics[metric_key]
                            
                            for list_key in ["erros", "detalhamento"]:
                                if list_key in date_endpoint_metrics and date_endpoint_metrics[list_key]:
                                    consolidated_metrics[key][list_key].extend(date_endpoint_metrics[list_key])
                            
                            if date_endpoint_metrics["status"] == "FALHA":
                                consolidated_metrics[key]["status"] = "FALHA"
                
                elif endpoint in ["rentabilidade", "extrato"]:
                    if endpoint in date_metrics:
                        date_endpoint_metrics = date_metrics[endpoint]
                        for metric_key in ["total_arquivos_processados", "total_registros_inseridos", 
                                         "total_fundos_unicos", "duracao_segundos"]:
                            if metric_key in date_endpoint_metrics and date_endpoint_metrics[metric_key]:
                                consolidated_metrics[endpoint][metric_key] = consolidated_metrics[endpoint].get(metric_key, 0) + date_endpoint_metrics[metric_key]
                        
                        for list_key in ["erros", "detalhamento", "detalhamento_por_fundo"]:
                            if list_key in date_endpoint_metrics and date_endpoint_metrics[list_key]:
                                consolidated_metrics[endpoint][list_key].extend(date_endpoint_metrics[list_key])
                        
                        if date_endpoint_metrics["status"] == "FALHA":
                            consolidated_metrics[endpoint]["status"] = "FALHA"
                    
        except Exception as e:
            error_msg = f"Erro ao processar data {data_ref}: {e}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            send_error_email(data_ref, error_msg, f"Processamento {data_ref}")

    end_global = datetime.datetime.now()
    duracao_global = (end_global - start_global).total_seconds()

    # ENVIO DO E-MAIL FINAL
    logger.info("=== PREPARANDO RELATÓRIO FINAL ===")
    
    # Determinar status geral baseado nos endpoints processados
    status_geral = "SUCESSO"
    for endpoint in args.endpoints:
        if endpoint == "carteira":
            if (consolidated_metrics["extracao"]["status"] == "FALHA" or 
                consolidated_metrics["processamento"]["status"] == "FALHA"):
                status_geral = "FALHA"
                break
        elif endpoint in ["rentabilidade", "extrato"]:
            if consolidated_metrics[endpoint]["status"] == "FALHA":
                status_geral = "FALHA"
                break

    # Preparar dados para o template
    processing_rows_carteira = build_processing_rows(consolidated_metrics["processamento"]["detalhamento"])
    processing_rows_rent = consolidated_metrics["rentabilidade"]["detalhamento"]
    processing_rows_extrato = build_processing_rows(consolidated_metrics["extrato"]["detalhamento"])
    detalhamento_por_fundo_rent = consolidated_metrics["rentabilidade"]["detalhamento_por_fundo"]

    date_range_display = f"{dates_to_process[0]} a {dates_to_process[-1]}" if len(dates_to_process) > 1 else dates_to_process[0]
    
    final_ctx = {
        "status": status_geral,
        "data_referencia": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "date_range": date_range_display,
        "total_dates_processed": len(dates_to_process),
        "endpoints_processados": ", ".join(args.endpoints).upper(),
        "duracao_total": f"{duracao_global:.1f} s",

        # Carteira
        "extracao_num_arquivos": consolidated_metrics["extracao"]["num_arquivos"] if "carteira" in args.endpoints else 0,
        "extracao_tamanho_total": f"{consolidated_metrics['extracao']['tamanho_bytes']/1024**2:.2f} MB" if "carteira" in args.endpoints else "0 MB",
        "extracao_duracao": f"{consolidated_metrics['extracao']['duracao_segundos']:.1f} s" if "carteira" in args.endpoints else "0 s",

        "processamento_total_arquivos": consolidated_metrics["processamento"]["total_arquivos_processados"] if "carteira" in args.endpoints else 0,
        "processamento_total_registros": consolidated_metrics["processamento"]["total_registros_inseridos"] if "carteira" in args.endpoints else 0,
        "processamento_duracao": f"{consolidated_metrics['processamento']['duracao_segundos']:.1f} s" if "carteira" in args.endpoints else "0 s",
        "processing_rows_carteira": processing_rows_carteira if "carteira" in args.endpoints else [],

        # Rentabilidade
        "rentabilidade_total_arquivos": consolidated_metrics["rentabilidade"]["total_arquivos_processados"] if "rentabilidade" in args.endpoints else 0,
        "rentabilidade_total_registros": consolidated_metrics["rentabilidade"]["total_registros_inseridos"] if "rentabilidade" in args.endpoints else 0,
        "rentabilidade_total_fundos": consolidated_metrics["rentabilidade"]["total_fundos_unicos"] if "rentabilidade" in args.endpoints else 0,
        "rentabilidade_duracao": f"{consolidated_metrics['rentabilidade']['duracao_segundos']:.1f} s" if "rentabilidade" in args.endpoints else "0 s",
        "processing_rows_rent": processing_rows_rent if "rentabilidade" in args.endpoints else [],
        "detalhamento_por_fundo_rent": detalhamento_por_fundo_rent if "rentabilidade" in args.endpoints else [],

        # Extrato
        "extrato_total_arquivos": consolidated_metrics["extrato"]["total_arquivos_processados"] if "extrato" in args.endpoints else 0,
        "extrato_total_registros": consolidated_metrics["extrato"]["total_registros_inseridos"] if "extrato" in args.endpoints else 0,
        "extrato_duracao": f"{consolidated_metrics['extrato']['duracao_segundos']:.1f} s" if "extrato" in args.endpoints else "0 s",
        "processing_rows_extrato": processing_rows_extrato if "extrato" in args.endpoints else [],

        # Seções extras
        "skipped_files_section": "",
        "critical_funds_section": "",
        "missing_funds_section": "",
        "errors_section": "",

        "execution_id": datetime.datetime.now().isoformat(),
        "log_path": str(log_file_path)
    }

    # Adicionar seção de erros
    all_errors = []
    for endpoint in args.endpoints:
        if endpoint == "carteira":
            for key in ["extracao", "processamento"]:
                if consolidated_metrics[key]["erros"]:
                    all_errors.extend([f"Carteira ({key.title()}): {err}" for err in consolidated_metrics[key]["erros"]])
        elif endpoint in ["rentabilidade", "extrato"]:
            if consolidated_metrics[endpoint]["erros"]:
                all_errors.extend([f"{endpoint.title()}: {err}" for err in consolidated_metrics[endpoint]["erros"]])
    
    if all_errors:
        final_ctx["errors_section"] = "<ul>" + "".join([f"<li>{err}</li>" for err in all_errors]) + "</ul>"

    # Definir assunto do email
    endpoints_str = " + ".join([ep.title() for ep in args.endpoints])
    if len(dates_to_process) > 1:
        subject_prefix = f"ETL BTG {endpoints_str} ({len(dates_to_process)} datas)"
    else:
        subject_prefix = f"ETL BTG {endpoints_str} ({date_range_display})"
        
    if status_geral == "SUCESSO":
        assunto = f"✅ SUCESSO {subject_prefix}"
    else:
        assunto = f"🚨 FALHA {subject_prefix} - Verificar Logs"

    # Log do resumo final
    logger.info("=== RESUMO FINAL ===")
    logger.info(f"Status geral: {status_geral}")
    logger.info(f"Datas processadas: {len(dates_to_process)}")
    logger.info(f"Endpoints processados: {args.endpoints}")
    logger.info(f"Duração total: {duracao_global:.1f}s")
    
    if "carteira" in args.endpoints:
        logger.info(f"Carteira: {consolidated_metrics['extracao']['num_arquivos']} arquivos, {consolidated_metrics['processamento']['total_registros_inseridos']} registros")
    if "rentabilidade" in args.endpoints:
        logger.info(f"Rentabilidade: {consolidated_metrics['rentabilidade']['total_arquivos_processados']} arquivos, {consolidated_metrics['rentabilidade']['total_registros_inseridos']} registros, {consolidated_metrics['rentabilidade']['total_fundos_unicos']} fundos únicos")
    if "extrato" in args.endpoints:
        logger.info(f"Extrato: {consolidated_metrics['extrato']['total_arquivos_processados']} arquivos, {consolidated_metrics['extrato']['total_registros_inseridos']} registros")

    # Envio do email (se configurado)
    if RECEIVER_EMAIL:
        try:
            logger.info("Enviando relatório por email...")
            notification_manager.send_with_template(
                TemplateNotification(
                    type=NotificationType.EMAIL,
                    recipients=RECEIVER_EMAIL,
                    subject=assunto,
                    template_path=str(EMAIL_TEMPLATES_DIR / "btg_carteira_report.html"),
                    context=final_ctx,
                    kwargs={"is_html": True}
                )
            )
            logger.info("Relatório enviado por email com sucesso")
        except Exception as e:
            logger.error(f"Falha ao enviar email: {e}")
    else:
        logger.warning("Nenhum destinatário de email configurado - pulando envio")

    logger.info("=== ORQUESTRADOR BTG FINALIZADO ===")
    sys.exit(0 if status_geral == "SUCESSO" else 1)

if __name__ == "__main__":
    main()