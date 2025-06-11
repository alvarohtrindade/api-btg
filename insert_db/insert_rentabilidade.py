#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para inserir dados de rentabilidade de fundos BTG no banco de dados MySQL.

Autor: Álvaro – Equipe Data Analytics – Catalise Investimentos
Data: 29/05/2025
Versão: 1.2.1 (corrigido parsing de JSON e métricas de saída)
"""

import os
import sys
import json
import argparse
import glob
import time
import tempfile
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

# Configuração de logs
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
hoje_str = datetime.now().strftime("%Y%m%d")
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
Log.set_log_file(str(LOGS_DIR / f"insert_rentabilidade_{hoje_str}.log"))
logger = Log.get_logger(__name__)

# Carrega variáveis de ambiente
from dotenv import load_dotenv
load_dotenv()

MYSQL_TABLE = os.getenv("DB_RENTABILIDADE")

def get_memory_usage_mb() -> float:
    """Retorna o uso atual de memória em MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def log_progress_dashboard(step: str, files_processed: int, records_processed: int, 
                          duration: float, memory_mb: float):
    """Exibe dashboard de progresso no console."""
    throughput = records_processed / duration if duration > 0 else 0
    
    logger.info(f"{'='*60}")
    logger.info(f"🔄 Rentabilidade BTG - {step}")
    logger.info(f"{'='*60}")
    logger.info(f"📁 Arquivos processados: {files_processed}")
    logger.info(f"📊 Registros processados: {records_processed:,}")
    logger.info(f"⏱️  Duração: {duration:.1f}s")
    logger.info(f"⚡ Throughput: {throughput:.1f} reg/s")
    logger.info(f"💾 Memória: {memory_mb:.1f} MB")
    logger.info(f"{'='*60}")

def converter_porcentagem_para_decimal(valor):
    """Converte um valor em porcentagem para decimal (divide por 100)."""
    if valor is None:
        return None
    try:
        return float(valor) / 100
    except (ValueError, TypeError):
        return None

def processar_json_rentabilidade(file_path: str, debug: bool = False) -> pd.DataFrame:
    """
    Processa um arquivo JSON de rentabilidade e retorna um DataFrame estruturado.
    Versão corrigida para lidar melhor com estruturas de JSON variadas.
    """
    try:
        logger.info(f"Processando arquivo JSON: {Path(file_path).name}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        resultados = []
        
        # Verificar se o JSON tem a estrutura esperada
        if not isinstance(data, dict):
            logger.warning(f"JSON não é um dicionário: {file_path}")
            return pd.DataFrame()
            
        result_data = data.get("result", [])
        
        if not isinstance(result_data, list):
            logger.warning(f"Campo 'result' não é uma lista: {file_path}")
            return pd.DataFrame()
            
        if len(result_data) == 0:
            logger.info(f"JSON sem dados de fundos: {file_path}")
            return pd.DataFrame()
            
        logger.info(f"Processando {len(result_data)} fundos do arquivo {Path(file_path).name}")

        for i, fundo in enumerate(result_data):
            if not isinstance(fundo, dict):
                logger.warning(f"Fundo {i} não é um dicionário")
                continue
                
            nome_fundo = fundo.get("fundName", "")
            if not nome_fundo:
                logger.warning(f"Fundo {i} sem nome")
                continue
                
            fund_data = fundo.get("data", [])
            if not isinstance(fund_data, list) or len(fund_data) == 0:
                logger.info(f"Fundo '{nome_fundo}' sem dados")
                continue
                
            for j, registro in enumerate(fund_data):
                if not isinstance(registro, dict):
                    logger.warning(f"Registro {j} do fundo '{nome_fundo}' não é um dicionário")
                    continue
                    
                try:
                    # Extrair rentabilidade nominal
                    profitability = registro.get("profitability", {})
                    rent_day = converter_porcentagem_para_decimal(profitability.get("day"))
                    rent_month = converter_porcentagem_para_decimal(profitability.get("month"))
                    rent_year = converter_porcentagem_para_decimal(profitability.get("year"))

                    # Extrair rentabilidade vs CDI
                    rent_vs_cdi_day = None
                    rent_vs_cdi_month = None
                    rent_vs_cdi_year = None
                    
                    quota_diff = registro.get("quotaProfitabilityDifference", {})
                    if isinstance(quota_diff, dict):
                        cdie_data = quota_diff.get("CDIE", {})
                        if isinstance(cdie_data, dict):
                            nominal_vs_indexador = cdie_data.get("NominalVsIndexador", {})
                            if isinstance(nominal_vs_indexador, dict):
                                rent_vs_cdi_day = converter_porcentagem_para_decimal(nominal_vs_indexador.get("Day"))
                                rent_vs_cdi_month = converter_porcentagem_para_decimal(nominal_vs_indexador.get("Month"))
                                rent_vs_cdi_year = converter_porcentagem_para_decimal(nominal_vs_indexador.get("Year"))

                    # Extrair data de referência
                    data_ref = registro.get("referenceDate", "")
                    if data_ref and "T" in data_ref:
                        data_ref = data_ref.split("T")[0]

                    row = {
                        "NmFundo": nome_fundo,
                        "CdConta": registro.get("account"),
                        "DocFundo": registro.get("cnpj"),
                        "DtPosicao": data_ref,
                        "VlrCotacao": registro.get("liquidQuote"),
                        "VlrCotacaoBruta": registro.get("rawQuote"),
                        "VlrPatrimonio": registro.get("assetValue"),
                        "QtdCota": registro.get("numberOfQuotes"),
                        "VlrAplicacao": registro.get("acquisitions"),
                        "VlrResgate": registro.get("redemptions"),
                        "RentDia": rent_day,
                        "RentMes": rent_month,
                        "RentAno": rent_year,
                        "RentDiaVsCDI": rent_vs_cdi_day,
                        "RentMesVsCDI": rent_vs_cdi_month,
                        "RentAnoVsCDI": rent_vs_cdi_year,
                        "TpClasse": registro.get("hierarchyClass"),
                        "arquivo_origem": Path(file_path).name
                    }
                    resultados.append(row)
                    
                except Exception as e:
                    logger.error(f"Erro ao processar registro {j} do fundo '{nome_fundo}': {e}")
                    continue

        if not resultados:
            logger.info(f"Nenhum registro válido extraído de {file_path}")
            return pd.DataFrame()

        df = pd.DataFrame(resultados)
        
        # Garantir o formato de data
        if "DtPosicao" in df.columns:
            df["DtPosicao"] = pd.to_datetime(df["DtPosicao"], errors="coerce").dt.strftime("%Y-%m-%d")
            
        logger.info(f"Extraídos {len(df)} registros do arquivo {Path(file_path).name}")
        return df

    except Exception as e:
        logger.error(f"Erro ao processar JSON {file_path}: {e}")
        return pd.DataFrame()

def bulk_insert_rentabilidade_optimized(df_all: pd.DataFrame, conn: MySQLConnector):
    """Insere todo o DataFrame usando execute_dataframe_insert para máxima performance."""
    if df_all.empty:
        return 0, 0.0

    start_insert = time.time()
    
    try:
        # Remove a coluna arquivo_origem antes da inserção
        df_insert = df_all.drop(columns=['arquivo_origem'], errors='ignore')
        
        # Usar o método otimizado do connector para inserção em lote
        inserted_count = conn.execute_dataframe_insert(
            df_insert, 
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

def process_all_files_optimized(pasta_json: Path) -> tuple[List[pd.DataFrame], List[Dict[str, Any]]]:
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
            df_parcial = processar_json_rentabilidade(arquivo)
            t1 = time.time()
            dur_arquivo = round(t1 - t0, 3)

            if df_parcial is None or df_parcial.empty:
                detalhes.append({
                    "Arquivo": nome_arquivo,
                    "Data Processo": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Total Linhas": 0,
                    "Inseridos": 0,
                    "Duração (s)": dur_arquivo,
                    "Status": "SEM DADOS"
                })
                logger.info(f"⚠️ {nome_arquivo}: sem dados válidos")
                continue

            num_linhas = len(df_parcial)
            data_frames.append(df_parcial)

            detalhes.append({
                "Arquivo": nome_arquivo,
                "Data Processo": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Total Linhas": num_linhas,
                "Inseridos": num_linhas,  # Será ajustado após inserção
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
    parser = argparse.ArgumentParser(description="Insert Rentabilidade BTG no MySQL")
    parser.add_argument("--json-dir", type=str, required=True,
                        help="Diretório contendo os arquivos .json de rentabilidade.")
    parser.add_argument("--auto", action="store_true",
                        help="Executa sem prompt interativo.")
    parser.add_argument("--chunk-size", type=int, default=5000,
                        help="Tamanho dos lotes para inserção (padrão: 5000)")
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

    # Conectar ao MySQL
    try:
        conn = MySQLConnector.from_env()
        logger.info("✅ Conexão MySQL estabelecida com sucesso")
    except Exception as e:
        error_msg = f"Erro ao conectar ao MySQL: {e}"
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
        data_frames, detalhes = process_all_files_optimized(pasta_json)
        
        if not data_frames:
            error_msg = "Nenhum JSON gerou dados válidos para inserção."
            logger.warning(error_msg)
            
            metrics_no_data = {
                "status": "SUCESSO",  # Mudado para SUCESSO pois pode ser normal não ter dados
                "total_arquivos_processados": len(detalhes),
                "total_registros_inseridos": 0,
                "duracao_segundos": time.time() - start_total,
                "detalhamento": detalhes,
                "erros": []
            }
            print(json.dumps(metrics_no_data, ensure_ascii=False))
            sys.exit(0)

        # Consolidar todos os DataFrames
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

        # Inserção em bulk otimizada
        logger.info(f"🚀 Iniciando inserção em bulk de {len(df_all):,} registros...")
        
        total_inseridos, dur_insert = bulk_insert_rentabilidade_optimized(df_all, conn)
        
        log_progress_dashboard(
            "Inserção no Banco", 
            len(data_frames), 
            total_inseridos, 
            dur_insert,
            get_memory_usage_mb()
        )

        # Atualizar detalhamento com proporções dos registros inseridos
        total_linhas_processadas = sum(item["Total Linhas"] for item in detalhes if item["Total Linhas"] > 0)
        
        if total_linhas_processadas > 0:
            for item in detalhes:
                if item.get("Status") == "PROCESSADO":
                    # Calcular proporção de registros inseridos baseada no tamanho do arquivo
                    proporcao = item["Total Linhas"] / total_linhas_processadas
                    item["Inseridos"] = int(total_inseridos * proporcao)
                    item["Status"] = "SUCESSO"
                elif item["Total Linhas"] == 0:
                    item["Inseridos"] = 0
                    if "SEM DADOS" in item["Status"]:
                        item["Status"] = "IGNORADO - SEM DADOS"

        # Calcular duração total
        duracao_total = time.time() - start_total
        status_geral = "SUCESSO"
        erros_gerais = []

        # Log de resumo final
        logger.info(f"🎉 Processamento de Rentabilidade concluído com SUCESSO!")
        logger.info(f"📁 Arquivos processados: {len(detalhes)}")
        logger.info(f"📊 Registros inseridos: {total_inseridos:,}")
        logger.info(f"⏱️  Tempo total: {duracao_total:.1f}s")
        logger.info(f"⚡ Throughput médio: {total_inseridos/duracao_total:.1f} reg/s")

        # Métricas finais - formato compatível com o orquestrador
        metrics_out = {
            "status": status_geral,
            "total_arquivos_processados": len(detalhes),
            "total_registros_inseridos": total_inseridos,
            "duracao_segundos": duracao_total,
            "detalhamento": detalhes,
            "erros": erros_gerais
        }

        print(json.dumps(metrics_out, ensure_ascii=False))
        sys.exit(0)

    except Exception as e:
        duracao_total = time.time() - start_total
        error_msg = f"Erro crítico durante processamento: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Atualizar detalhamento com erro geral
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
        # Fechar conexão
        try:
            conn.close()
            logger.info("🔌 Conexão MySQL fechada")
        except Exception as e:
            logger.warning(f"Erro ao fechar conexão: {e}")

if __name__ == "__main__":
    main()