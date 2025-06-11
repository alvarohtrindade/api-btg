#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Inserção de dados de carteira diária BTG no banco MySQL

Autor: Álvaro - Equipe Data Analytics - Catalise Investimentos
Data: 29/05/2025
Versão: 1.3.0  (adaptado para manter conexão única, usar JSON de tipos de fundo e gerar tabela de detalhamento)
"""

import os
import sys
import json
import argparse
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd

# —————————————————————————————————————————————————————————————
# Ajustar ROOT_PATH para garantir que 'utils' esteja no sys.path
# Caminho real de insert_carteira.py:
#    catalise/DataAnalytics/backend/api_btg/insert_db/insert_carteira.py
# Portanto, subindo 3 níveis, chegamos em: catalise/DataAnalytics
ROOT_PATH = Path(__file__).resolve().parents[3]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

# Agora podemos importar os utilitários
from dotenv import load_dotenv

from utils.logging_utils import Log, LogLevel
from utils.mysql_connector_utils import MySQLConnector, QueryError
from utils.json_utils import ConfigValidator, InvalidJsonError

# —————————————————————————————————————————————————————————————
# Configuração de logs (mantida do original)
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
Log.set_log_file(str(LOGS_DIR / f"insert_carteira_{datetime.now().strftime('%Y%m%d')}.log"))
logger = Log.get_logger(__name__)

# Carrega variáveis de ambiente via .env (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE etc.)
load_dotenv()

# Diretórios auxiliares (se você usar SQL_DIR e SCHEMAS_DIR em outras partes)
SQL_DIR = ROOT_PATH / "sql"
SCHEMAS_DIR = ROOT_PATH / "schemas"

# —————————————————————————————————————————————————————————————
# >>>> Novos diretórios para JSONs de mapeamento:
MAPPINGS_DIR = ROOT_PATH / "configs" / "mappings"
COLUMN_MAPPING_JSON    = MAPPINGS_DIR / "column_mapping.json"
FUND_MAPPING_JSON      = MAPPINGS_DIR / "fund_mapping.json"
DESCRICAO_MAPPING_JSON = MAPPINGS_DIR / "descricao_mapping.json"
GRUPO_MAPPING_JSON     = MAPPINGS_DIR / "grupo_mapping.json"
FUND_TYPE_MAPPING_JSON = MAPPINGS_DIR / "fund_type_mapping.json"  # JSON contendo {"fund_type_mapping": {"Nome Fundo A": "FIA", ...}}

# Carrega qualquer JSON pequeno:
def load_json(path: Path) -> Dict[str, Any]:
    """
    Carrega um JSON (sem validação Pydantic), devolvendo um dict.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Falha ao carregar JSON de mapeamento: {path} -> {e}")
        return {}

# Carrega todos os dicionários de mapeamento assim que o script inicia:
column_map_data = load_json(COLUMN_MAPPING_JSON)
column_mapping = column_map_data.get("column_mapping", {})             # ex.: {"Nome Fundo":"NmFundo", ...}
drop_columns   = column_map_data.get("drop_columns", [])               # ex.: ["Quota"]

fund_map_data     = load_json(FUND_MAPPING_JSON)
fund_mapping      = fund_map_data.get("fund_mapping", {})              # dicionário de fund_mapping

descricao_map_data = load_json(DESCRICAO_MAPPING_JSON)
descricao_mapping  = descricao_map_data.get("descricao_mapping", {})   # dicionário de descricoes

grupo_map_data    = load_json(GRUPO_MAPPING_JSON)
grupo_mapping     = grupo_map_data.get("grupo_mapping", {})            # dicionário de grupo_map

fund_type_map_data = load_json(FUND_TYPE_MAPPING_JSON)
fund_type_mapping  = fund_type_map_data.get("fund_type_mapping", {})    # dicionário: NomeFundo → tipo (FIA, FIDC, FIC_FIM, etc.)

# —————————————————————————————————————————————————————————————
# Aqui começam as funções legadas de leitura e processamento que você forneceu:

def mapear_nomes_fic(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Remove espaços extras em 'NmFundo' e aplica o dicionário de mapeamento de nomes de fundo.
    """
    df['NmFundo'] = df['NmFundo'].str.strip()
    df['NmFundo'] = df['NmFundo'].replace(mapping)
    return df

def ajustar_quantidade_legado(valor: Any) -> Any:
    """
    Formata a quantidade com separador de milhar e 4 casas decimais.
    """
    try:
        if valor is None or valor == "":
            return valor
        valor_num = float(valor)
        # Formata com ponto como separador de milhar e vírgula para decimal
        valor_formatado = f"{valor_num:,.4f}".replace('.', 'X').replace(',', '.').replace('X', ',')
        return valor_formatado
    except Exception as e:
        logger.warning(f"Erro ao ajustar quantidade '{valor}': {e}")
        return valor

def process_portfolio_investido(df: pd.DataFrame, nome_fundo: str, data: str) -> (Optional[pd.DataFrame], Optional[List[str]]):
    try:
        start_index = df[df[df.columns[0]] == 'Portfolio_Investido'].index[0]
        end_index   = df[df[df.columns[0]] == 'DESPESAS'].index[0]
        portfolio_df = df.loc[start_index:end_index].iloc[1:-3].reset_index(drop=True)

        new_column_names = portfolio_df.iloc[0].tolist()
        portfolio_df.columns = new_column_names
        portfolio_df = portfolio_df[1:].reset_index(drop=True)
        portfolio_df.insert(0, 'Nome Fundo', nome_fundo)
        portfolio_df.insert(1, 'Data', data)
        portfolio_df = portfolio_df.iloc[:, :9].drop(columns=['ISIN', 'CNPJ', '% P.L.'])
        portfolio_df['Classificacao'] = 'PORTFOLIO INVESTIDO'
        portfolio_df['TpFundo'] = None
        portfolio_df['Descricao'] = None
        portfolio_df['Cod'] = grupo_mapping.get('PORTFOLIO INVESTIDO', None)

        return portfolio_df, new_column_names
    except Exception as e:
        logger.error(f"Erro ao processar Portfolio Investido do fundo '{nome_fundo}': {e}")
        return None, None

def process_titulos_publicos(df: pd.DataFrame, nome_fundo: str, data: str, new_column_names: List[str]) -> Optional[pd.DataFrame]:
    try:
        start_index = df[df[df.columns[0]] == 'Titulos_Publicos'].index[0]
        end_index   = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        titulos_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        titulos_df.columns = new_column_names
        titulos_df['CNPJ'] = ''
        titulos_df['Quantidade'] = ''
        titulos_df['Quota'] = ''
        titulos_df['Portfólio Inv.'] = titulos_df['Financeiro']
        titulos_df['Financeiro'] = titulos_df['Var.Diária']
        titulos_df.iloc[:, titulos_df.columns.get_loc('% P.L.')] = titulos_df.iloc[:, -2]
        titulos_df = titulos_df.drop(columns=['ISIN']).iloc[:, :6].drop(index=0)
        titulos_df.insert(0, 'Nome Fundo', nome_fundo)
        titulos_df.insert(1, 'Data', data)
        titulos_df['Classificacao'] = 'RENDA FIXA'
        titulos_df['TpFundo'] = None
        titulos_df['Descricao'] = None
        titulos_df['Cod'] = grupo_mapping.get('RENDA FIXA', None)
        return titulos_df
    except Exception as e:
        logger.error(f"Erro ao processar Títulos Públicos do fundo '{nome_fundo}': {e}")
        return None

def process_acoes(df: pd.DataFrame, nome_fundo: str, data: str, new_column_names: List[str]) -> Optional[pd.DataFrame]:
    try:
        start_index = df[df[df.columns[0]] == 'Acoes'].index[0]
        end_index   = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        acoes_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        acoes_df.columns = new_column_names
        acoes_df['Portfólio Inv.'] = acoes_df['Quantidade']
        acoes_df['Quantidade'] = acoes_df['Quota']
        acoes_df['Quota'] = acoes_df['Financeiro']
        acoes_df['Financeiro'] = acoes_df['% P.L.']
        acoes_df = acoes_df.drop(columns=['ISIN', 'CNPJ', '% P.L.']).iloc[:, :4].drop(index=0)
        acoes_df.insert(0, 'Nome Fundo', nome_fundo)
        acoes_df.insert(1, 'Data', data)
        acoes_df['Classificacao'] = 'ACOES'
        acoes_df['TpFundo'] = None
        acoes_df['Descricao'] = None
        acoes_df['Cod'] = grupo_mapping.get('OUTROS', None)
        return acoes_df
    except Exception as e:
        logger.error(f"Erro ao processar Ações do fundo '{nome_fundo}': {e}")
        return None

def process_despesas(df: pd.DataFrame, nome_fundo: str, data: str) -> Optional[pd.DataFrame]:
    try:
        start_index = df[df[df.columns[0]] == 'DESPESAS'].index[0]
        despesas_df = df.loc[start_index:].iloc[1:, :4]
        new_column_names = despesas_df.iloc[0].tolist()
        despesas_df.columns = new_column_names
        despesas_df = despesas_df[1:].reset_index(drop=True)
        despesas_df = despesas_df.rename(columns={'Nome': 'Portfólio Inv.', 'Valor': 'Financeiro'})
        despesas_df = despesas_df.drop(columns=['Data Início Vigência', 'Data Fim Vigência'])
        despesas_df.insert(0, 'Nome Fundo', nome_fundo)
        despesas_df.insert(1, 'Data', data)
        despesas_df['Classificacao'] = 'DESPESAS'
        despesas_df['TpFundo'] = None
        despesas_df['Descricao'] = None
        despesas_df['Cod'] = grupo_mapping.get('DESPESAS', None)
        return despesas_df
    except Exception as e:
        logger.error(f"Erro ao processar Despesas do fundo '{nome_fundo}': {e}")
        return None

def process_caixa(df: pd.DataFrame, nome_fundo: str, data: str) -> Optional[pd.DataFrame]:
    try:
        caixa_row = df[df[df.columns[0]] == 'C/C SALDO FUNDO'].index[0]
        financeiro_value = df.iloc[caixa_row, 1]
        caixa_df = pd.DataFrame({
            'Nome Fundo': [nome_fundo],
            'Data': [data],
            'Portfólio Inv.': ['C/C SALDO FUNDO'],
            'Financeiro': [financeiro_value],
            'Classificacao': ['CAIXA'],
            'TpFundo': [None],
            'Descricao': [None],
            'Cod': [grupo_mapping.get('SALDO DE CAIXA', None)]
        })
        return caixa_df
    except Exception as e:
        logger.error(f"Erro ao processar Caixa do fundo '{nome_fundo}': {e}")
        return None

def process_titulos_privados(df: pd.DataFrame, nome_fundo: str, data: str) -> Optional[pd.DataFrame]:
    try:
        start_index = df[df[df.columns[0]] == 'Titulos_Privados'].index[0]
        end_index   = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        titulos_privados_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        titulos_privados_df.columns = titulos_privados_df.iloc[0]
        titulos_privados_df = titulos_privados_df[1:]
        titulos_privados_df = titulos_privados_df[['Data', 'Vencimento', 'Quantidade', 'Título', 'Financeiro']]
        titulos_privados_df = titulos_privados_df.rename(columns={'Data': 'DataAplicacao', 'Título': 'Portfólio Inv.'})
        titulos_privados_df.insert(0, 'Nome Fundo', nome_fundo)
        titulos_privados_df.insert(1, 'Data', data)
        titulos_privados_df['Classificacao'] = 'RENDA FIXA'
        titulos_privados_df['DataAplicacao'] = pd.to_datetime(titulos_privados_df['DataAplicacao'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        titulos_privados_df['Vencimento']    = pd.to_datetime(titulos_privados_df['Vencimento'],    dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        titulos_privados_df = titulos_privados_df.fillna('')
        titulos_privados_df['TpFundo'] = None
        titulos_privados_df['Descricao'] = None
        titulos_privados_df['Cod'] = grupo_mapping.get('RENDA FIXA', None)
        return titulos_privados_df
    except Exception as e:
        logger.error(f"Erro ao processar Títulos Privados do fundo '{nome_fundo}': {e}")
        return None

def titulos_publicos(df: pd.DataFrame, nome_fundo: str, data: str) -> Optional[pd.DataFrame]:
    try:
        start_index = df[df[df.columns[0]] == 'Titulos_Publicos'].index[0]
        end_index   = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        publicos_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        publicos_df.columns = publicos_df.iloc[0]
        publicos_df = publicos_df[1:]
        publicos_df = publicos_df[['Data', 'Vencimento', 'Quantidade', 'Título', 'Financeiro']]
        publicos_df = publicos_df.rename(columns={'Data': 'DataAplicacao', 'Título': 'Portfólio Inv.'})
        publicos_df.insert(0, 'Nome Fundo', nome_fundo)
        publicos_df.insert(1, 'Data', data)
        publicos_df['Classificacao'] = 'RENDA FIXA'
        publicos_df['DataAplicacao'] = pd.to_datetime(publicos_df['DataAplicacao'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        publicos_df['Vencimento']    = pd.to_datetime(publicos_df['Vencimento'],    dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        publicos_df = publicos_df.fillna('')
        publicos_df['TpFundo'] = None
        publicos_df['Descricao'] = None
        publicos_df['Cod'] = grupo_mapping.get('RENDA FIXA', None)
        return publicos_df
    except Exception as e:
        logger.error(f"Erro ao processar Títulos Públicos do fundo '{nome_fundo}': {e}")
        return None

def extract_and_format_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Lê o DataFrame já carregado do Excel e extrai/torce/formata
    exatamente como o script legado faz, retornando um único DataFrame final.
    Retorna None se não houver dados válidos a extrair.
    """
    try:
        # 1) Nome do fundo e Data de posição (essas posições vêm sempre no Excel legado)
        nome_fundo = df.iloc[5, 0].replace('_', ' ')
        data_pos   = df.iloc[6, 1]

        # 2) Extrair cada bloco de informação (Legacy):
        portfolio_df, new_column_names = process_portfolio_investido(df, nome_fundo, data_pos)
        if portfolio_df is None or portfolio_df.empty:
            return None

        col0 = df[df.columns[0]]

        titulos_df = None
        if 'Titulos_Publicos' in col0.values:
            titulos_df = process_titulos_publicos(df, nome_fundo, data_pos, new_column_names)

        acoes_df = None
        if 'Acoes' in col0.values:
            acoes_df = process_acoes(df, nome_fundo, data_pos, new_column_names)

        despesas_df = None
        if 'DESPESAS' in col0.values:
            despesas_df = process_despesas(df, nome_fundo, data_pos)

        caixa_df = None
        if 'C/C SALDO FUNDO' in col0.values:
            caixa_df = process_caixa(df, nome_fundo, data_pos)

        publicos_df = None
        if 'Titulos_Publicos' in col0.values:
            publicos_df = titulos_publicos(df, nome_fundo, data_pos)

        titulos_privados_df = None
        if 'Titulos_Privados' in col0.values:
            titulos_privados_df = process_titulos_privados(df, nome_fundo, data_pos)

        # 3) Agora concatenar somente os DataFrames que efetivamente foram gerados.
        lista_para_concat = []
        for bloco in (
            portfolio_df,
            titulos_df,
            acoes_df,
            despesas_df,
            caixa_df,
            publicos_df,
            titulos_privados_df
        ):
            if isinstance(bloco, pd.DataFrame) and not bloco.empty:
                lista_para_concat.append(bloco)

        if not lista_para_concat:
            return None

        final_df = pd.concat(lista_para_concat, ignore_index=True)
        final_df = final_df.replace('nan', '').fillna('')

        return final_df

    except Exception as e:
        logger.error(f"Erro ao extrair e formatar dados do fundo (extract_and_format_data): {e}", exc_info=True)
        return None

def read_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Lê o Excel em pandas e cria um CSV temporário para evitar problemas com certos formatos.
    """
    df = pd.read_excel(file_path)
    temp_csv_path = file_path.with_suffix(".temp.csv")
    df.to_csv(temp_csv_path, index=False, header=True)
    df = pd.read_csv(temp_csv_path)
    temp_csv_path.unlink(missing_ok=True)
    return df

def process_files(input_directory: Path) -> pd.DataFrame:
    """
    Varre todos os arquivos .xlsx na pasta, invoca 'extract_and_format_data' e concatena tudo num único DataFrame.
    Não mais utilizado diretamente, mas mantido para referência.
    """
    files = [f for f in os.listdir(input_directory) if f.lower().endswith(".xlsx")]
    all_dataframes = []
    for file in files:
        file_path = input_directory / file
        logger.info(f"Processando arquivo (legado): {file_path.name}")
        df_raw = read_excel_file(file_path)
        df_legado = extract_and_format_data(df_raw)
        if df_legado is not None and not df_legado.empty:
            all_dataframes.append(df_legado)
        else:
            logger.warning(f"Legado retornou None ou DataFrame vazio para {file_path.name}")
    if all_dataframes:
        return pd.concat(all_dataframes, ignore_index=True)
    else:
        return pd.DataFrame()

# —————————————————————————————————————————————————————————————
# Função auxiliar para extrair tipo de fundo (agora obsoleta, pois usaremos JSON)
FUNDO_TP_REGEX = r'^(CDB|LCA|LCI|LF|CRA|CRI|Debentures)\s*([A-Za-z\s]+)$'

# —————————————————————————————————————————————————————————————

def process_file(file_path: Path,
                 connector: MySQLConnector,
                 schema_validator: Optional[ConfigValidator]) -> Dict[str, Any]:
    """
    process_file agora retorna um dict com:
      - inserted_rows: int
      - total_rows:    int
      - duration_s:    float
      - status:        "SUCESSO" ou "ERRO: ..."
    """
    detalhe: Dict[str, Any] = {
        "Arquivo": file_path.name,
        "Data Processo": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Total Linhas": 0,
        "Inseridos": 0,
        "Duração (s)": 0.0,
        "Status": "OK"
    }

    start_file = time.time()
    try:
        # 1) Carrega apenas este arquivo Excel
        df_raw    = read_excel_file(file_path)

        # 2) Extrai/torce/concatena tudo via legado
        df_legacy = extract_and_format_data(df_raw)

        if df_legacy is None or df_legacy.empty:
            logger.info(f"[{file_path.name}] Legado não retornou dados.")
            detalhe["Status"] = "SEM DADOS"
            return detalhe

        # 4) Renomeia colunas conforme mapeamento fixo
        df_renamed = df_legacy.rename(columns=column_mapping)
        for col in drop_columns:
            if col in df_renamed.columns:
                df_renamed = df_renamed.drop(columns=[col])

        # 5) Insere coluna fixa 'Custodiante'
        df_renamed['Custodiante'] = 'BTG'

        # 6) Aplicar JSONs de mapeamento ponto a ponto:
        #    6a) Fundos (NmFundo) → fund_mapping.json
        if 'NmFundo' in df_renamed.columns:
            df_renamed = mapear_nomes_fic(df_renamed, fund_mapping)

        #    6b) Ajusta Qnt via função legado (formatação numérica)
        if 'Qnt' in df_renamed.columns:
            df_renamed['Qnt'] = df_renamed['Qnt'].apply(ajustar_quantidade_legado)

        #    6c) Tipo de Fundo via JSON de fund_type_mapping.json
        if 'NmFundo' in df_renamed.columns:
            df_renamed['TpFundo'] = df_renamed['NmFundo'].map(fund_type_mapping).fillna('OUTROS')

        #    6d) Descrição (mapa de Grupos → descricao_mapping.json)
        if 'Grupo' in df_renamed.columns:
            df_renamed['Descricao'] = df_renamed['Grupo'].map(descricao_mapping).fillna(df_renamed['Grupo'])

        #    6e) Cod (mapa de Grupos → grupo_mapping.json)
        if 'Grupo' in df_renamed.columns:
            df_renamed['Cod'] = df_renamed['Grupo'].map(grupo_mapping).fillna(df_renamed['Cod'] if 'Cod' in df_renamed.columns else None)

        # 7) Ajustar a data DtPosicao (YYYY-MM-DD)
        if 'DtPosicao' in df_renamed.columns:
            df_renamed['DtPosicao'] = pd.to_datetime(
                df_renamed['DtPosicao'], dayfirst=True, errors='coerce'
            ).dt.strftime('%Y-%m-%d')

        # 8) Reordenar colunas conforme o schema (se schema_validator existir)
        if schema_validator is not None:
            schema_dict = schema_validator.config
            if 'target_columns' in schema_dict and isinstance(schema_dict['target_columns'], list):
                ordered_cols = [c for c in schema_dict['target_columns'] if c in df_renamed.columns]
                df_final = df_renamed[ordered_cols]
            else:
                df_final = df_renamed
        else:
            df_final = df_renamed

        # 9) Validar via JSON/schema (se schema_validator existir)
        if schema_validator is not None:
            erros = schema_validator.validate_dataframe(df_final)
            if erros:
                raise InvalidJsonError(f"Erros de validação em {file_path.name}: {erros}")

        # 10) Preparar dados para detalhamento
        detalhe["Total Linhas"] = int(len(df_final))

        # 11) Inserir no MySQL
        if not df_final.empty:
            inserted = insert_data_to_mysql(df_final, connector)
            detalhe["Inseridos"] = int(inserted)
            logger.info(f"[{file_path.name}] {inserted} registros inseridos.")
        else:
            logger.info(f"[{file_path.name}] DataFrame final vazio → nada a inserir.")
            detalhe["Status"] = "SEM REGISTROS PARA INSERIR"

    except InvalidJsonError as e:
        logger.error(f"[{file_path.name}] Erro de schema: {e}", exc_info=True)
        detalhe["Status"] = f"ERRO SCHEMA: {e}"

    except Exception as e:
        logger.error(f"[{file_path.name}] Erro geral ao processar: {e}", exc_info=True)
        detalhe["Status"] = f"ERRO GERAL: {e}"

    end_file = time.time()
    detalhe["Duração (s)"] = round(end_file - start_file, 3)
    return detalhe

def insert_data_to_mysql(df: pd.DataFrame, connector: MySQLConnector) -> int:
    """
    Insere as linhas do DataFrame na tabela 'carteira_btg_diaria'
    usando batch insert via MySQLConnector.execute_dataframe_insert.
    Retorna o total de linhas inseridas.
    Observação: NÃO fecha o connector aqui — será fechado no final do loop em main().
    """
    if df.empty:
        return 0

    table_name = os.getenv("MYSQL_TABLE", "Ft_CarteiraDiaria")
    try:
        inserted_count = connector.execute_dataframe_insert(df, table_name, batch_size=500)
        logger.info(f"Inseridos {inserted_count} registros em {table_name}.")
        return inserted_count
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {table_name}: {e}", exc_info=True)
        raise

def main():
    parser = argparse.ArgumentParser(description="Script de inserção de carteira BTG no MySQL")
    parser.add_argument('--date', '-d', dest='date_ref', required=True, help='Data de referência (YYYY-MM-DD)')
    parser.add_argument('--input-dir', '-i', dest='input_dir', required=True, help='Diretório de entrada com os arquivos')
    args = parser.parse_args()

    date_ref  = args.date_ref
    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(f"Diretório de entrada inválido: {input_dir}")
        sys.exit(1)

    # Carrega schema
    schema_json = SCHEMAS_DIR / 'schema_carteira_btg.json'
    if schema_json.exists():
        try:
            schema_validator = ConfigValidator(str(schema_json))
        except InvalidJsonError as e:
            logger.error(f"Falha ao carregar schema: {e}")
            schema_validator = None
    else:
        schema_validator = None

    # Conecta ao MySQL
    try:
        connector = MySQLConnector.from_env()
    except Exception as e:
        logger.error(f"Não foi possível conectar ao MySQL: {e}", exc_info=True)
        sys.exit(1)

    total_files_processed = 0
    total_registros_inseridos = 0
    detalhamento_por_arquivo: List[Dict[str, Any]] = []

    start_proc = time.time()

    # ────────────────────────────────────────────────────────────────────
    # Itera EM CADA ARQUIVO .xlsx dentro de 'input_dir'
    # ────────────────────────────────────────────────────────────────────
    for file_path in sorted(input_dir.iterdir()):
        if file_path.suffix.lower() != ".xlsx":
            logger.info(f"Ignorando (não é .xlsx): {file_path.name}")
            continue

        if not file_path.is_file():
            logger.info(f"Ignorando (não é arquivo): {file_path.name}")
            continue

        total_files_processed += 1
        detalhe = process_file(file_path, connector, schema_validator)
        detalhamento_por_arquivo.append(detalhe)
        # Soma somente se foi bem-sucedido (inseridos é int)
        try:
            total_registros_inseridos += int(detalhe.get("Inseridos", 0))
        except:
            pass

    end_proc = time.time()
    duracao_proc = round(end_proc - start_proc, 1)

    # Fecha a conexão APÓS processar todos os arquivos
    try:
        connector.close()
    except Exception as e:
        logger.warning(f"Erro ao fechar conexão MySQL: {e}")

    # Monta métricas finais (incluindo detalhamento)
    output_metrics = {
        "total_arquivos_processados": total_files_processed,
        "total_registros_inseridos": total_registros_inseridos,
        "duracao_segundos": duracao_proc,
        "detalhamento": detalhamento_por_arquivo
    }

    print(json.dumps(output_metrics, default=str))
    sys.exit(0)

if __name__ == "__main__":
    main()
