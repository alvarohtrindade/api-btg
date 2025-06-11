"""
File: date_utils.py
Author: Cesar Godoy
Date: 2025-04-06
Version: 1.0
Description: Utilitário para obtenção de datas úteis para calendário financeiro,
             consultando banco de dados para validar feriados e fins de semana.
"""

import datetime
import os
import sys
import argparse
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from utils.logging_utils import Log, LogLevel
from utils.mysql_connector_utils import MySQLConnector, QueryError, ConnectionError

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / '.env')

# Inicialização do log
os.makedirs('logs', exist_ok=True)
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
Log.set_log_file('logs/date_utils.log', append=True)

# Logger específico para este módulo
logger = Log.get_logger(__name__)


def get_mysql_connector() -> MySQLConnector:
    """
    Inicializa e retorna um conector MySQL com base nas variáveis de ambiente.
   
    Returns:
        Instância de MySQLConnector configurada.
   
    Raises:
        ConnectionError: Em caso de erro ao inicializar a conexão.
    """
    required_vars = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ConnectionError(f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing_vars)}")
    
    try:
        os.makedirs('logs', exist_ok=True)
        return MySQLConnector.from_env(
            log_file='logs/date_utils_mysql.log',
            log_level=LogLevel.INFO
        )
    
    except Exception as e:
        raise ConnectionError(f"Erro ao inicializar conexão MySQL: {str(e)}") from e


def get_calendar(connector: MySQLConnector, reference_date: datetime.date, n_days_back: int = 1) -> datetime.date:
    """
    Obtém a data útil correspondente a n_days_back dias úteis atrás a partir da data de referência.
   
    Args:
        connector: Conexão MySQL ativa.
        reference_date: Data de referência.
        n_days_back: Quantos dias úteis retroceder. Para a data útil atual, use 1. Para o dia
                     útil anterior, use 2, e assim por diante.
   
    Returns:
        Data útil encontrada no calendário.
   
    Raises:
        Exception: Caso não encontre a data ou ocorra erro na consulta.
    """
    try:
        query = '''
        SELECT DtReferencia
          FROM vw_calendario
         WHERE DtReferencia <= %s
           AND Feriado = 0
           AND FimSemana = 0
         ORDER BY DtReferencia DESC
         LIMIT %s, 1
        '''
        logger.info(f"Executando query de calendário com referência={reference_date}, n_days_back={n_days_back}")

        results = connector.execute_query(query, (reference_date, n_days_back))

        if results:
            return results[0]['DtReferencia']

        raise Exception(f"Não foi possível encontrar o {n_days_back} dia útil anterior a {reference_date}")

    except QueryError as e:
        raise Exception(f"Erro ao consultar o banco de dados: {str(e)}") from e


def get_reference_business_day(
    connector: MySQLConnector,
    n_days: Optional[int] = None,
    specific_date: Optional[str] = None
) -> datetime.date:
    """
    Retorna a data de referência com base em uma data específica ou no número de dias úteis.
   
    Args:
        connector: Conexão MySQL.
        n_days: Dias úteis a retroceder a partir da data atual.
        specific_date: Data específica no formato YYYY-MM-DD.
   
    Returns:
        Data de referência válida.
   
    Raises:
        ValueError: Em caso de formato inválido ou erro no cálculo da data.
    """
    if n_days is not None and n_days < 0:
        raise ValueError(f"O parâmetro n_days deve ser maior ou igual a zero, valor recebido: {n_days}")
        
    if specific_date:
        try:
            ref_date = datetime.datetime.strptime(specific_date, '%Y-%m-%d').date()
        
        except ValueError as e:
            raise ValueError(
                f"Formato de data inválido: \"{specific_date}\". Use o formato YYYY-MM-DD (ex: 2025-03-30)."
            ) from e
        
    else:
        ref_date = datetime.date.today()

    try:
        # Passamos n_days_back + 1 porque queremos retroceder "n_days + 1" dias úteis
        # (contando a partir de 0, onde 0 é o dia atual)
        return get_calendar(connector, ref_date, (n_days or 0) + 1)
    
    except Exception as e:
        raise ValueError(
            f"Erro ao calcular o {n_days or 0} dia útil anterior a {ref_date}: {str(e)}"
        ) from e


def get_business_day(
        n_days: Optional[int] = None, 
        specific_date: Optional[str] = None
    ) -> datetime.date:
    """
    Resolve a data de referência e retorna a data útil correspondente.
   
    Args:
        n_days: Número de dias úteis a retroceder.
        specific_date: Data fixa fornecida pelo usuário.
   
    Returns:
        Data útil resultante.
   
    Raises:
        ValueError: Em caso de entrada inválida.
    """
    connector = None
    try:
        connector = get_mysql_connector()
        return get_reference_business_day(connector, n_days, specific_date)
    
    finally:
        if connector:
            connector.close()