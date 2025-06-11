#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para inserir dados de Caixa Extrato BTG no banco de dados MySQL.

Autor: Álvaro – Equipe Data Analytics – Catalise Investimentos
Data: 01/06/2025
Versão: 1.1.0 (Corrigido para usar schema de validação)
"""

import os
import sys
import json
import argparse
import glob
import time
import psutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

# Ajustar ROOT_DIR para que utils fique disponível
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.logging_utils import Log, LogLevel
from utils.mysql_connector_utils import MySQLConnector, QueryError
from utils.json_utils import ConfigValidator, InvalidJsonError

# Configuração de logs
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
hoje_str = datetime.now().strftime("%Y%m%d")
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
Log.set_log_file(str(LOGS_DIR / f"insert_extrato_{hoje_str}.log"))
logger = Log.get_logger(__name__)

# Carrega variáveis de ambiente
from dotenv import load_dotenv
load_dotenv()

MYSQL_TABLE = os.getenv("DB_EXTRATO", "despesas_fundos")

# Diretórios de configuração
SCHEMAS_DIR = ROOT_DIR / "schemas"
MAPPINGS_DIR = ROOT_DIR / "configs" / "mappings"

def get_memory_usage_mb() -> float:
    """Retorna o uso atual de memória em MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def log_progress_dashboard(step: str, files_processed: int, records_processed: int, 
                          duration: float, memory_mb: float):
    """Exibe dashboard de progresso no console."""
    throughput = records_processed / duration if duration > 0 else 0
    
    logger.info(f"{'='*60}")
    logger.info(f"🔄 Caixa Extrato BTG - {step}")
    logger.info(f"{'='*60}")
    logger.info(f"📁 Arquivos processados: {files_processed}")
    logger.info(f"📊 Registros processados: {records_processed:,}")
    logger.info(f"⏱️  Duração: {duration:.1f}s")
    logger.info(f"⚡ Throughput: {throughput:.1f} reg/s")
    logger.info(f"💾 Memória: {memory_mb:.1f} MB")
    logger.info(f"{'='*60}")

def load_extrato_mapping() -> Dict[str, Any]:
    """Carrega o mapeamento de extrato do arquivo JSON."""
    mapping_file = MAPPINGS_DIR / "extrato_mapping.json"
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Erro ao carregar mapeamento de {mapping_file}: {e}")
        # Fallback para mapeamento hardcoded
        return {
            "column_mapping": {
                "assetName": "nmfundo",
                "operationDate": "data",
                "history": "lancamento",
                "balance": "valor",
                "credit": "VlrCredito",
                "debt": "VlrDebito",
                "observation": "observacao"
            },
            "fund_name_mapping": {}
        }

def convert_valor_monetario(value) -> Optional[float]:
    """Converte valores monetários para float, tratando casos especiais."""
    if value is None or value == "" or pd.isna(value):
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Erro ao converter valor monetário: {value}")
        return None

def determinar_tipo_lancamento(credit: Optional[float], debt: Optional[float]) -> str:
    """Determina o tipo de lançamento baseado nos valores de crédito e débito."""
    if credit and credit > 0:
        return "CREDITO"
    elif debt and debt > 0:
        return "DEBITO"
    else:
        return "OUTROS"

def categorizar_lancamento(history: str) -> str:
    """Categoriza o lançamento baseado no histórico."""
    if not history:
        return "OUTROS"
    
    history_upper = history.upper()
    
    categorias = {
        "APLICACAO": ["APLICACAO", "APORTE", "DEPOSITO"],
        "RESGATE": ["RESGATE", "SAQUE", "RETIRADA"],
        "TAXA": ["TAXA", "TARIFA", "COBRANCA"],
        "RENDIMENTO": ["RENDIMENTO", "JUROS", "RENTABILIDADE"],
        "TRANSFERENCIA": ["TRANSFERENCIA", "DOC", "TED"]
    }
    
    for categoria, palavras in categorias.items():
        if any(palavra in history_upper for palavra in palavras):
            return categoria
    
    return "OUTROS"

def processar_json_extrato(file_path: str, mapping: Dict[str, Any]) -> pd.DataFrame:
    """
    Processa um arquivo JSON de caixa extrato e retorna um DataFrame estruturado.
    """
    try:
        logger.info(f"Processando arquivo JSON: {Path(file_path).name}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        resultados = []
        
        if not isinstance(data, dict):
            logger.warning(f"JSON não é um dicionário: {file_path}")
            return pd.DataFrame()
            
        result_data = data.get("result", [])
        
        if not isinstance(result_data, list) or len(result_data) == 0:
            logger.info(f"JSON sem dados de extrato: {file_path}")
            return pd.DataFrame()
            
        logger.info(f"Processando {len(result_data)} registros do arquivo {Path(file_path).name}")

        column_mapping = mapping.get("column_mapping", {})
        fund_name_mapping = mapping.get("fund_name_mapping", {})

        for i, registro in enumerate(result_data):
            if not isinstance(registro, dict):
                continue
                
            try:
                # Extrair dados básicos
                asset_name = registro.get("assetName", "")
                operation_date = registro.get("operationDate", "")
                history = registro.get("history", "")
                balance = registro.get("balance")
                credit = registro.get("credit")
                debt = registro.get("debt")
                
                # Aplicar mapeamento de nome do fundo
                nome_fundo = fund_name_mapping.get(asset_name, asset_name)
                
                # Processar data
                if operation_date and "T" in operation_date:
                    data_operacao = operation_date.split("T")[0]
                else:
                    data_operacao = operation_date or None

                # Converter valores monetários
                valor_credito = convert_valor_monetario(credit)
                valor_debito = convert_valor_monetario(debt)
                valor_saldo = convert_valor_monetario(balance)
                
                # Determinar valor principal
                if valor_saldo is not None:
                    valor_principal = valor_saldo
                elif valor_credito and valor_debito:
                    valor_principal = max(valor_credito, valor_debito)
                elif valor_credito:
                    valor_principal = valor_credito
                elif valor_debito:
                    valor_principal = valor_debito
                else:
                    valor_principal = 0.0

                # Determinar tipo e categoria
                tipo_lancamento = determinar_tipo_lancamento(valor_credito, valor_debito)
                categoria = categorizar_lancamento(history)

                # Extrair ano e mês
                ano = None
                mes = None
                if data_operacao:
                    try:
                        dt = pd.to_datetime(data_operacao)
                        ano = dt.year
                        mes = dt.strftime('%B')
                    except:
                        pass

                # Montar registro conforme schema da tabela despesas_fundos
                registro_final = {
                    "data": data_operacao,
                    "nmfundo": nome_fundo,
                    "nmcategorizado": nome_fundo,
                    "lancamento": history or "N/A",
                    "lancamento_original": history or "N/A",
                    "valor": valor_principal,
                    "tipo_lancamento": tipo_lancamento,
                    "categoria": categoria,
                    "observacao": registro.get("observation", ""),
                    "custodiante": "BTG",
                    "TpFundo": "EXTRATO",
                    "ano": ano,
                    "mes": mes
                }
                
                resultados.append(registro_final)
                    
            except Exception as e:
                logger.error(f"Erro ao processar registro {i}: {e}")
                continue

        if not resultados:
            return pd.DataFrame()

        df = pd.DataFrame(resultados)
        
        # Garantir formato de data
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.strftime("%Y-%m-%d")
            
        # Filtrar registros válidos
        df = df.dropna(subset=["nmfundo", "lancamento", "valor"])
        
        logger.info(f"Extraídos {len(df)} registros válidos do arquivo {Path(file_path).name}")
        return df

    except Exception as e:
        logger.error(f"Erro ao processar JSON {file_path}: {e}")
        return pd.DataFrame()

def bulk_insert_extrato_optimized(df_all: pd.DataFrame, conn: MySQLConnector, schema_validator: Optional[ConfigValidator] = None):
    """Insere todo o DataFrame usando execute_dataframe_insert para máxima performance."""
    if df_all.empty:
        return 0, 0.0

    start_insert = time.time()
    
    try:
        # Validar schema se disponível
        if schema_validator:
            erros = schema_validator.validate_dataframe(df_all)
            if erros:
                logger.warning(f"Avisos de validação de schema: {erros}")
                # Não falha, apenas alerta
        
        # Validar colunas essenciais
        required_columns = ["data", "nmfundo", "lancamento", "valor", "tipo_lancamento"]
        missing_columns = [col for col in required_columns if col not in df_all.columns]
        
        if missing_columns:
            logger.error(f"Colunas obrigatórias ausentes: {missing_columns}")
            return 0, 0.0
        
        # Garantir que não há valores nulos nas colunas críticas
        df_clean = df_all.dropna(subset=["nmfundo", "lancamento", "valor"])
        
        if df_clean.empty:
            logger.warning("Nenhum registro válido após limpeza de dados")
            return 0, 0.0
        
        logger.info(f"Inserindo {len(df_clean)} registros válidos (de {len(df_all)} originais)")
        
        # Inserção em lote
        inserted_count = conn.execute_dataframe_insert(
            df_clean, 
            MYSQL_TABLE, 
            batch_size=5000
        )
        
        end_insert = time.time()
        dur = end_insert - start_insert
        
        logger.info(f"✅ Bulk insert concluído: {inserted_count:,} registros em {dur:.1f}s "
                    f"({inserted_count/dur:.1f} reg/s)")
        
        return inserted_count, dur

    except Exception as e:
        logger.error(f"Erro na inserção em lote: {e}")
        raise

def process_all_files_optimized(pasta_json: Path, mapping: Dict[str, Any]) -> tuple[List[pd.DataFrame], List[Dict[str, Any]]]:
    """Processa todos os arquivos JSON de uma vez de forma otimizada."""
    arquivos_json = sorted(glob.glob(str(pasta_json / "*.json")))
    
    if not arquivos_json:
        logger.warning("Nenhum arquivo JSON encontrado")
        return [], []
    
    logger.info(f"🔄 Processando {len(arquivos_json)} arquivos JSON...")
    
    data_frames: List[pd.DataFrame] = []
    detalhes: List[Dict[str, Any]] = []
    
    start_processing = time.time()
    
    for arquivo in arquivos_json:
        nome_arquivo = Path(arquivo).name
        t0 = time.time()
        
        try:
            df_parcial = processar_json_extrato(arquivo, mapping)
            t1 = time.time()
            dur_arquivo = round(t1 - t0, 3)

            if df_parcial is None or df_parcial.empty:
                detalhes.append({
                    "Arquivo": nome_arquivo,
                    "Data Processo": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Total Linhas": 0,
                    "Inseridos": 0,
                    "Duração (s)": dur_arquivo,
                    "Status": "SEM DADOS VÁLIDOS"
                })
                logger.info(f"⚠️ {nome_arquivo}: sem dados válidos")
                continue

            num_linhas = len(df_parcial)
            data_frames.append(df_parcial)

            detalhes.append({
                "Arquivo": nome_arquivo,
                "Data Processo": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Total Linhas": num_linhas,
                "Inseridos": num_linhas,
                "Duração (s)": dur_arquivo,
                "Status": "PROCESSADO"
            })
            
            logger.info(f"✅ {nome_arquivo}: {num_linhas:,} registros processados em {dur_arquivo:.3f}s")
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar {nome_arquivo}: {e}")
            detalhes.append({
                "Arquivo": nome_arquivo,
                "Data Processo": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Total Linhas": 0,
                "Inseridos": 0,
                "Duração (s)": 0,
                "Status": f"ERRO: {str(e)}"
            })
    
    end_processing = time.time()
    processing_duration = end_processing - start_processing
    total_records = sum(len(df) for df in data_frames)
    
    log_progress_dashboard(
        "Processamento de JSONs", 
        len(data_frames), 
        total_records, 
        processing_duration,
        get_memory_usage_mb()
    )
    
    return data_frames, detalhes

def main():
    parser = argparse.ArgumentParser(description="Insert Caixa Extrato BTG no MySQL")
    parser.add_argument("--json-dir", type=str, required=True,
                        help="Diretório contendo os arquivos .json de caixa extrato.")
    parser.add_argument("--auto", action="store_true",
                        help="Executa sem prompt interativo.")
    parser.add_argument("--chunk-size", type=int, default=5000,
                        help="Tamanho dos lotes para inserção (padrão: 5000)")
    parser.add_argument("--debug", action="store_true",
                        help="Ativa modo debug com logs detalhados")
    args = parser.parse_args()

    pasta_json = Path(args.json_dir)
    if not pasta_json.exists() or not pasta_json.is_dir():
        error_msg = f"Diretório {pasta_json} não existe."
        logger.error(error_msg)
        
        metrics_fail = {
            "status": "FALHA",
            "total_arquivos_processados": 0,
            "total_registros_inseridos": 0,
            "duracao_segundos": 0,
            "detalhamento": [],
            "erros": [error_msg]
        }
        print(json.dumps(metrics_fail, ensure_ascii=False))
        sys.exit(1)

    start_total = time.time()

    # Carregar schema de validação
    schema_json = SCHEMAS_DIR / 'schema_extrato_btg.json'
    schema_validator = None
    if schema_json.exists():
        try:
            schema_validator = ConfigValidator(str(schema_json))
            logger.info("Schema de validação carregado com sucesso")
        except InvalidJsonError as e:
            logger.error(f"Falha ao carregar schema: {e}")
    else:
        logger.warning("Schema de validação não encontrado, prosseguindo sem validação")

    # Carregar mapeamento de colunas
    mapping = load_extrato_mapping()
    logger.info("Mapeamento de colunas carregado")

    # Conectar ao MySQL
    try:
        conn = MySQLConnector.from_env()
        logger.info("✅ Conexão MySQL estabelecida com sucesso")
        
        # Testar tabela
        test_query = f"SELECT COUNT(*) FROM {MYSQL_TABLE} LIMIT 1"
        conn.query_single_value(test_query)
        logger.info(f"✅ Tabela {MYSQL_TABLE} acessível")
        
    except Exception as e:
        error_msg = f"Erro ao conectar ao MySQL ou acessar tabela {MYSQL_TABLE}: {e}"
        logger.error(error_msg)
        
        metrics_fail = {
            "status": "FALHA",
            "total_arquivos_processados": 0,
            "total_registros_inseridos": 0,
            "duracao_segundos": 0,
            "detalhamento": [],
            "erros": [error_msg]
        }
        print(json.dumps(metrics_fail, ensure_ascii=False))
        sys.exit(1)

    try:
        # Processar todos os arquivos
        data_frames, detalhes = process_all_files_optimized(pasta_json, mapping)
        
        if not data_frames:
            logger.warning("Nenhum JSON gerou dados válidos para inserção.")
            
            metrics_no_data = {
                "status": "SUCESSO",
                "total_arquivos_processados": len(detalhes),
                "total_registros_inseridos": 0,
                "duracao_segundos": time.time() - start_total,
                "detalhamento": detalhes,
                "erros": []
            }
            print(json.dumps(metrics_no_data, ensure_ascii=False))
            sys.exit(0)

        # Consolidar DataFrames
        logger.info(f"🔄 Consolidando {len(data_frames)} DataFrames...")
        start_consolidation = time.time()
        
        df_all = pd.concat(data_frames, ignore_index=True)
        
        end_consolidation = time.time()
        consolidation_duration = end_consolidation - start_consolidation
        
        logger.info(f"✅ Consolidação concluída: {len(df_all):,} registros em {consolidation_duration:.1f}s")
        
        log_progress_dashboard(
            "Consolidação", 
            len(data_frames), 
            len(df_all), 
            consolidation_duration,
            get_memory_usage_mb()
        )

        # Inserção em bulk
        logger.info(f"🚀 Iniciando inserção em bulk de {len(df_all):,} registros...")
        
        total_inseridos, dur_insert = bulk_insert_extrato_optimized(df_all, conn, schema_validator)
        
        log_progress_dashboard(
            "Inserção no Banco", 
            len(data_frames), 
            total_inseridos, 
            dur_insert,
            get_memory_usage_mb()
        )

        # Atualizar detalhamento
        total_linhas_processadas = sum(item["Total Linhas"] for item in detalhes if item["Total Linhas"] > 0)
        
        if total_linhas_processadas > 0:
            for item in detalhes:
                if item.get("Status") == "PROCESSADO":
                    proporcao = item["Total Linhas"] / total_linhas_processadas
                    item["Inseridos"] = int(total_inseridos * proporcao)
                    item["Status"] = "SUCESSO"
                elif item["Total Linhas"] == 0:
                    item["Inseridos"] = 0
                    if "SEM DADOS" in item["Status"]:
                        item["Status"] = "IGNORADO - SEM DADOS"

        # Métricas finais
        duracao_total = time.time() - start_total
        status_geral = "SUCESSO" if total_inseridos > 0 else "FALHA"

        logger.info(f"🎉 Processamento de Caixa Extrato concluído!")
        logger.info(f"📁 Arquivos processados: {len(detalhes)}")
        logger.info(f"📊 Registros inseridos: {total_inseridos:,}")
        logger.info(f"⏱️  Tempo total: {duracao_total:.1f}s")
        logger.info(f"🎯 Status final: {status_geral}")

        metrics_out = {
            "status": status_geral,
            "total_arquivos_processados": len(detalhes),
            "total_registros_inseridos": total_inseridos,
            "duracao_segundos": duracao_total,
            "detalhamento": detalhes,
            "erros": []
        }

        print(json.dumps(metrics_out, ensure_ascii=False))
        sys.exit(0 if status_geral == "SUCESSO" else 1)

    except Exception as e:
        duracao_total = time.time() - start_total
        error_msg = f"Erro crítico durante processamento: {str(e)}"
        logger.error(error_msg, exc_info=True)

        if 'detalhes' in locals():
            for item in detalhes:
                if item.get("Status") == "PROCESSADO":
                    item["Status"] = f"ERRO INSERÇÃO: {str(e)}"

        metrics_error = {
            "status": "FALHA",
            "total_arquivos_processados": len(detalhes) if 'detalhes' in locals() else 0,
            "total_registros_inseridos": 0,
            "duracao_segundos": duracao_total,
            "detalhamento": detalhes if 'detalhes' in locals() else [],
            "erros": [error_msg]
        }

        print(json.dumps(metrics_error, ensure_ascii=False))
        sys.exit(1)

    finally:
        try:
            conn.close()
            logger.info("🔌 Conexão MySQL fechada")
        except Exception as e:
            logger.warning(f"Erro ao fechar conexão: {e}")

if __name__ == "__main__":
    main()