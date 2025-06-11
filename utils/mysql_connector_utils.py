"""
File: mysql_connector_utils.py
Author: Cesar Godoy
Date: 2025-04-08
Version: 1.0
Description: Utilitário para conexão MySQL com pool de conexões, 
             backoff, validação de queries e logging integrado.
             Responsável apenas pela conexão e execução de consultas básicas.
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from contextlib import contextmanager

# Adiciona o diretório raiz ao sys.path ao rodar diretamente
if __name__ == '__main__' and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Bibliotecas de terceiros
import mysql.connector
from mysql.connector import pooling
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

# Módulos internos
from utils.logging_utils import Log, LogLevel
from utils.backoff_utils import with_backoff_jitter, RetryExhaustedError


# Exceções personalizadas para o conector MySQL
class MySQLError(Exception):
    """Exceção base para erros relacionados ao MySQL."""
    pass


class QueryError(MySQLError):
    """Exceção para erros durante a execução de consultas SQL."""
    pass


class ConnectionError(MySQLError):
    """Exceção para erros de conexão com o banco de dados."""
    pass


# Carregar variáveis de ambiente
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / '.env')


#
# Definições de Classes de Configuração
#
@dataclass
class QueryMetrics:
    """Métricas de execução para consultas SQL."""
    query_type: str
    execution_time: float
    affected_rows: int = 0
    retries: int = 0
    success: bool = True
    error_message: Optional[str] = None


class MySQLConfig(BaseModel):
    """Configuração para conexão MySQL."""
    host: str
    port: int = 3306
    database: str
    user: str
    password: str
    charset: str = 'utf8mb4'
    connect_timeout: int = 10
    pool_size: int = 2
    pool_name: str = 'mypool'
    max_retries: int = 3
    base_wait: float = 1.0
    jitter_factor: float = 0.5
    use_pure: bool = True
    
    @field_validator('pool_size')
    def validate_pool_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError('O tamanho do pool deve ser maior que zero')
        return v
    
    @field_validator('max_retries')
    def validate_max_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError('O número máximo de retentativas não pode ser negativo')
        return v
    
    @field_validator('jitter_factor')
    def validate_jitter_factor(cls, v: float) -> float:
        if not 0 <= v <= 1:
            raise ValueError('O fator de jitter deve estar entre 0 e 1')
        return v


#
# Classe Principal do Conector MySQL
#
class MySQLConnector:
    """
    Conector MySQL com suporte a pool de conexões, retry, validação de queries.
    Responsável apenas pelas funcionalidades de conexão e execução de consultas.
    """
    
    def __init__(
        self, 
        config: Optional[MySQLConfig] = None,
        log_file: Optional[str] = None,
        log_level: LogLevel = LogLevel.INFO
    ):
        """
        Inicializa o conector MySQL.
        
        Args:
            config: Configuração para conexão (opcional, usa variáveis env se não fornecido)
            log_file: Caminho para arquivo de log (opcional)
            log_level: Nível de log (padrão: INFO)
        """
        # Configura o logging
        Log.set_level(log_level)
        Log.set_console_output(True)
        
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            Log.set_log_file(log_file, append=True)
        
        # Se não foi fornecida configuração, tenta carregar do .env
        if config is None:
            config = self._load_config_from_env()
        
        self.config = config
        self._pool = None
        self._initialized = False
        
        Log.info(
            f"MySQLConnector inicializado para {config.host}:{config.port}/{config.database}", 
            name='MySQLConnector'
        )
    
    def _load_config_from_env(self) -> MySQLConfig:
        """
        Carrega configuração a partir de variáveis de ambiente.
        
        Returns:
            MySQLConfig: Objeto de configuração preenchido
            
        Raises:
            ValueError: Se variáveis obrigatórias estiverem ausentes
        """
        try:
            # Variáveis obrigatórias
            host = os.getenv('MYSQL_HOST')
            database = os.getenv('MYSQL_DATABASE')
            user = os.getenv('MYSQL_USER')
            password = os.getenv('MYSQL_PASSWORD')
            
            if not all([host, database, user, password]):
                missing = [var for var, val in {
                    'MYSQL_HOST': host, 
                    'MYSQL_DATABASE': database,
                    'MYSQL_USER': user, 
                    'MYSQL_PASSWORD': password
                }.items() if not val]
                
                error_message = f"Variáveis de ambiente ausentes: {', '.join(missing)}"
                Log.error(error_message, name='MySQLConnector')
                raise ValueError(error_message)
            
            # Variáveis opcionais
            port = int(os.getenv('MYSQL_PORT', '3306'))
            charset = os.getenv('MYSQL_CHARSET', 'utf8mb4')
            connect_timeout = int(os.getenv('MYSQL_CONNECT_TIMEOUT', '10'))
            pool_size = int(os.getenv('MYSQL_POOL_SIZE', '5'))
            pool_name = os.getenv('MYSQL_POOL_NAME', 'mypool')
            max_retries = int(os.getenv('MYSQL_MAX_RETRIES', '3'))
            base_wait = float(os.getenv('MYSQL_BASE_WAIT', '1.0'))
            jitter_factor = min(1.0, max(0.0, float(os.getenv('MYSQL_JITTER_FACTOR', '0.5'))))
            use_pure = os.getenv('MYSQL_USE_PURE', 'True').lower() == 'true'
            
            return MySQLConfig(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                charset=charset,
                connect_timeout=connect_timeout,
                pool_size=pool_size,
                pool_name=pool_name,
                max_retries=max_retries,
                base_wait=base_wait,
                jitter_factor=jitter_factor,
                use_pure=use_pure
            )
            
        except Exception as e:
            error_message = f"Erro ao carregar configuração do MySQL: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise ValueError(error_message) from e
    
    def _initialize_pool(self) -> None:
        """
        Inicializa o pool de conexões.
        
        Raises:
            ConnectionError: Se ocorrer erro ao inicializar o pool
        """
        if self._initialized:
            return
            
        try:
            Log.info(f"Inicializando pool com {self.config.pool_size} conexões", name='MySQLConnector')
            
            dbconfig = {
                'host': self.config.host,
                'port': self.config.port,
                'user': self.config.user,
                'password': self.config.password,
                'database': self.config.database,
                'charset': self.config.charset,
                'use_pure': self.config.use_pure,
                'connection_timeout': self.config.connect_timeout
            }
            
            self._pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=self.config.pool_name,
                pool_size=self.config.pool_size,
                **dbconfig
            )
                
            self._initialized = True
            Log.info('Pool de conexões inicializado com sucesso', name='MySQLConnector')
            
        except Exception as e:
            error_message = f"Erro ao inicializar pool de conexões: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise ConnectionError(error_message) from e
    
    @contextmanager
    def get_connection(self):
        """
        Obtém uma conexão do pool e a fecha automaticamente ao término.
        
        Yields:
            Conexão MySQL
            
        Raises:
            ConnectionError: Se ocorrer erro ao obter conexão
        """
        if not self._initialized:
            self._initialize_pool()
            
        connection = None
        try:
            connection = self._pool.get_connection()
            yield connection
        except mysql.connector.Error as e:
            error_message = f"Erro ao obter conexão: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise ConnectionError(error_message) from e
        
        finally:
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    Log.warning(f"Erro ao fechar conexão: {str(e)}", name='MySQLConnector')
    
    @contextmanager
    def get_cursor(self, dictionary: bool = True):
        """
        Obtém um cursor a partir de uma conexão e o fecha automaticamente ao término.
        
        Args:
            dictionary: Se True, retorna resultados como dicionários (padrão: True)
            
        Yields:
            Cursor MySQL
            
        Raises:
            ConnectionError: Se ocorrer erro ao obter conexão ou cursor
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor(dictionary=dictionary)
                yield cursor

            except mysql.connector.Error as e:
                error_message = f"Erro ao criar cursor: {str(e)}"
                Log.error(error_message, name='MySQLConnector')
                raise ConnectionError(error_message) from e
            
            finally:
                try:
                    cursor.close()

                except Exception as e:
                    Log.warning(f"Erro ao fechar cursor: {str(e)}", name='MySQLConnector')
    
    def _extract_query_type(self, query: str) -> str:
        """
        Extrai o tipo da consulta SQL (SELECT, INSERT, etc.).
        
        Args:
            query: Consulta SQL
            
        Returns:
            str: Tipo da consulta (SELECT, INSERT, UPDATE, DELETE, etc.)
        """
        query = query.lstrip()
        words = query.split(' ', 1)
        if words:
            return words[0].upper()
        return 'UNKNOWN'
    
    def _is_retriable_error(self, exception: Exception) -> bool:
        """
        Verifica se uma exceção deve iniciar retry.
        
        Args:
            exception: Exceção a ser verificada
            
        Returns:
            bool: True se a exceção deve iniciar retry, False caso contrário
        """
        retriable_errors = (
            2003,  # Can't connect to MySQL server
            2006,  # MySQL server has gone away
            2013,  # Lost connection during query
            1040,  # Too many connections
            1205,  # Lock wait timeout
            1213,  # Deadlock
            1158,  # Communication packet error
        )
        
        if isinstance(exception, mysql.connector.Error):
            if hasattr(exception, 'errno') and exception.errno in retriable_errors:
                return True
            
        return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[Dict[str, Any], List[Any], Tuple[Any, ...]]] = None,
        fetch_all: bool = True
    ) -> Tuple[List[Dict[str, Any]], QueryMetrics]:
        """
        Executa uma consulta SQL com retry automático e retorno de métricas.

        Args:
            query: Consulta SQL
            params: Parâmetros para a consulta
            fetch_all: Se True, retorna todas as linhas

        Returns:
            Tupla (resultados, métricas)

        Raises:
            QueryError: Se ocorrer erro na execução da consulta
        """
        query_type = self._extract_query_type(query)
        start_time = time.time()

        maintenance_commands = ('CHECK', 'REPAIR', 'OPTIMIZE', 'ANALYZE')
        is_maintenance = query_type in maintenance_commands

        @with_backoff_jitter(
            max_attempts=self.config.max_retries + 1,
            base_wait=self.config.base_wait,
            jitter=self.config.jitter_factor,
            retryable_exceptions=(mysql.connector.Error,)
        )
        def _execute_query():
            with self.get_connection() as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(query, params)

                    # ⚠️ Primeiro consome o resultado principal
                    if fetch_all:
                        result = cursor.fetchall() or []
                    else:
                        row = cursor.fetchone()
                        result = [row] if row else []

                    # Consome todos os result sets adicionais (caso existam)
                    while cursor.nextset():
                        try:
                            if cursor.with_rows:
                                cursor.fetchall()
                        except Exception:
                            break

                    # Faz o commit, se necessário
                    if query_type not in ('SELECT', 'DESCRIBE', 'SHOW', 'EXPLAIN') or is_maintenance:
                        connection.commit()

                    affected_rows = cursor.rowcount if cursor.rowcount > 0 else 0
                    return result, affected_rows

        try:
            result, affected_rows = _execute_query()
            execution_time = time.time() - start_time

            metrics = QueryMetrics(
                query_type=query_type,
                execution_time=execution_time,
                affected_rows=affected_rows,
                success=True
            )

            return result, metrics

        except RetryExhaustedError as e:
            execution_time = time.time() - start_time
            error_message = f"Erro após todas as tentativas: {str(e)}"

            if "Unread result found" in str(e):
                Log.warning("Detectado erro 'Unread result found'. Reiniciando pool de conexões...", name='MySQLConnector')
                try:
                    self.close()
                    self._initialize_pool()
                    Log.info("Pool de conexões reiniciado com sucesso", name='MySQLConnector')
                except Exception as reset_error:
                    Log.error(f"Falha ao reiniciar pool: {str(reset_error)}", name='MySQLConnector')

            Log.error(f"Falha na consulta {query_type}: {error_message}", name='MySQLConnector')

            metrics = QueryMetrics(
                query_type=query_type,
                execution_time=execution_time,
                affected_rows=0,
                success=False,
                error_message=error_message,
                retries=self.config.max_retries
            )

            raise QueryError(error_message) from e

        except Exception as e:
            execution_time = time.time() - start_time
            error_message = f"Erro na consulta: {str(e)}"

            if "Unread result found" in str(e):
                Log.warning("Detectado erro 'Unread result found'. Reiniciando pool de conexões...", name='MySQLConnector')
                try:
                    self.close()
                    self._initialize_pool()
                    Log.info("Pool de conexões reiniciado com sucesso", name='MySQLConnector')
                except Exception as reset_error:
                    Log.error(f"Falha ao reiniciar pool: {str(reset_error)}", name='MySQLConnector')

            Log.error(f"Falha na consulta {query_type}: {error_message}", name='MySQLConnector')

            metrics = QueryMetrics(
                query_type=query_type,
                execution_time=execution_time,
                affected_rows=0,
                success=False,
                error_message=error_message
            )

            raise QueryError(error_message) from e

    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Union[Dict[str, Any], List[Any], Tuple[Any, ...]]] = None, 
        fetch_all: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SQL SELECT.
        
        Args:
            query: Consulta SQL (SELECT, DESCRIBE, etc.)
            params: Parâmetros para a consulta
            fetch_all: Se True, retorna todas as linhas
            
        Returns:
            Lista de resultados como dicionários
            
        Raises:
            ValueError: Se o tipo de consulta não for válido para esta operação
            QueryError: Se ocorrer erro na execução da consulta
        """
        query_type = self._extract_query_type(query)
        if query_type not in ('SELECT', 'DESCRIBE', 'SHOW', 'EXPLAIN'):
            raise ValueError(f"execute_query deve ser usado apenas para consultas, não para {query_type}")
            
        results, _ = self.execute(query, params, fetch_all)
        return results
    
    def execute_update(
        self, 
        query: str, 
        params: Optional[Union[Dict[str, Any], List[Any], Tuple[Any, ...]]] = None
    ) -> int:
        """
        Executa uma consulta SQL de modificação (INSERT, UPDATE, DELETE).
        
        Args:
            query: Consulta SQL (INSERT, UPDATE, DELETE)
            params: Parâmetros para a consulta
            
        Returns:
            int: Número de linhas afetadas
            
        Raises:
            ValueError: Se o tipo de consulta não for válido para esta operação
            QueryError: Se ocorrer erro na execução da consulta
        """
        query_type = self._extract_query_type(query)
        if query_type in ('SELECT', 'DESCRIBE', 'SHOW', 'EXPLAIN'):
            raise ValueError(f"execute_update deve ser usado apenas para modificações, não para {query_type}")
            
        _, metrics = self.execute(query, params, False)
        return metrics.affected_rows
    
    def execute_batch(
        self,
        query: str,
        params_list: List[Union[Dict[str, Any], List[Any], Tuple[Any, ...]]]
    ) -> int:
        """
        Executa uma série de operações em batch como uma transação única.
        
        Args:
            query: Consulta SQL
            params_list: Lista de conjuntos de parâmetros
            
        Returns:
            int: Número de linhas afetadas
            
        Raises:
            QueryError: Se ocorrer erro na execução do batch
        """
        if not params_list:
            return 0
            
        query_type = self._extract_query_type(query)
        start_time = time.time()
        
        try:
            with self.transaction() as connection:
                with connection.cursor() as cursor:
                    affected_rows = 0
                    for params in params_list:
                        cursor.execute(query, params)
                        affected_rows += cursor.rowcount
            
            # Calcula métricas
            execution_time = time.time() - start_time
            
            Log.info(
                f"Operação em batch {query_type} executada em {execution_time:.6f}s "
                f"({len(params_list)} operações, {affected_rows} linhas afetadas)",
                name='MySQLConnector'
            )
            
            return affected_rows 
            
        except Exception as e:
            error_message = f"Erro ao executar batch: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise QueryError(error_message) from e
    
    def execute_dataframe_insert(
        self, 
        df: 'pd.DataFrame', 
        table: str, 
        batch_size: int = 100
    ) -> int:
        """
        Insere os dados de um DataFrame em uma tabela MySQL como uma transação atômica,
        utilizando inserção em lote para melhor desempenho.
        
        Esta função utiliza transação para garantir que todas as linhas sejam inseridas
        como uma única operação, realizando rollback em caso de erro. As inserções são
        realizadas em lotes para melhorar o desempenho.
        
        Args:
            df: DataFrame com os dados a serem inseridos
            table: Nome da tabela destino
            batch_size: Tamanho dos lotes para inserção (padrão: 100)
            
        Returns:
            int: Número de registros inseridos
            
        Raises:
            QueryError: Se ocorrer erro na inserção dos dados
        """
        if df.empty:
            Log.warning(f"DataFrame vazio. Nenhum registro a ser inserido em {table}.", name='MySQLConnector')
            return 0
        
        # Obtém as colunas do DataFrame
        columns = df.columns.tolist()
        
        # Cria os placeholders para a query de valores individuais
        value_placeholder = f"({', '.join(['%s'] * len(columns))})"
        
        # Converte o DataFrame para uma lista de tuplas
        values = [tuple(row) for row in df.values]
        total_rows = len(values)
        
        # Registra operação
        Log.info(f"Preparando para inserir {total_rows} registros em {table} com lotes de {batch_size}", name='MySQLConnector')
        
        start_time = time.time()
        
        try:
            # Usa o gerenciador de contexto de transação para garantir atomicidade
            with self.transaction() as connection:
                with connection.cursor() as cursor:
                    affected_rows = 0
                    
                    # Processa em lotes
                    for i in range(0, total_rows, batch_size):
                        # Obtém o lote atual
                        batch_values = values[i:i + batch_size]
                        batch_size_actual = len(batch_values)
                        
                        # Constrói a query para inserção em lote
                        # Exemplo: INSERT INTO table (col1, col2) VALUES (%s, %s), (%s, %s), ...
                        placeholders = ', '.join([value_placeholder] * batch_size_actual)
                        batch_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES {placeholders}"
                        
                        # Prepara os parâmetros para a query (descompacta a lista de tuplas)
                        batch_params = [param for tuple_value in batch_values for param in tuple_value]
                        
                        # Executa a inserção em lote
                        cursor.execute(batch_query, batch_params)
                        rows_inserted = cursor.rowcount
                        affected_rows += rows_inserted
            
            # Calcula métricas
            execution_time = time.time() - start_time
            rows_per_second = total_rows / execution_time if execution_time > 0 else 0
            
            Log.info(
                f"DataFrame inserido com sucesso em {table}: {affected_rows} registros em {execution_time:.3f}s "
                f"({rows_per_second:.1f} registros/segundo)",
                name='MySQLConnector'
            )
            
            return affected_rows
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_message = f"Erro ao inserir DataFrame em {table}: {e}"
            
            Log.error(f"{error_message} (tempo decorrido: {execution_time:.3f}s)", name='MySQLConnector')
            
            # Se o erro for relacionado ao tamanho do lote, podemos tentar novamente com lotes menores
            if "packet too large" in str(e).lower() and batch_size > 10:
                new_batch_size = batch_size // 2
                Log.warning(
                    f"Pacote MySQL muito grande. Tentando novamente com tamanho de lote reduzido: {new_batch_size}",
                    name='MySQLConnector'
                )
                return self.execute_dataframe_insert(df, table, batch_size=new_batch_size)
                
            raise QueryError(error_message) from e
    
    @contextmanager
    def transaction(self):
        """
        Inicia uma transação e a gerencia automaticamente (commit/rollback).
        
        Yields:
            Conexão MySQL
            
        Raises:
            ConnectionError: Se ocorrer erro na obtenção da conexão
        """
        connection = None
        try:
            # Obtém uma conexão
            if not self._initialized:
                self._initialize_pool()
                
            connection = self._pool.get_connection()
            
            # Desabilita autocommit para iniciar uma transação
            connection.autocommit = False
            
            # Fornece a conexão para o bloco with
            yield connection
            
            # Se chegou aqui sem exceções, faz commit
            connection.commit()
            Log.info('Transação concluída com sucesso (commit)', name='MySQLConnector')
            
        except Exception as e:
            # Em caso de erro, faz rollback
            if connection:
                try:
                    connection.rollback()
                    Log.warning(f"Transação revertida (rollback): {str(e)}", name='MySQLConnector')

                except Exception as rollback_error:
                    Log.error(f"Erro ao fazer rollback: {str(rollback_error)}", name='MySQLConnector')
            
            # Propaga o erro
            error_message = f"Erro durante transação: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise QueryError(error_message) from e
            
        finally:
            # Fecha a conexão
            if connection:
                try:
                    # Restaura autocommit para estado padrão
                    connection.autocommit = True
                    
                except Exception:
                    Log.warning('Falha ao restaurar autocommit', name='MySQLConnector')

                finally:
                    connection.close()
    
    # Métodos utilitários básicos
    def query_to_dict(
        self, 
        query: str, 
        params: Optional[Any] = None, 
        key_field: str = 'id'
    ) -> Dict[Any, Dict[str, Any]]:
        """
        Executa uma consulta e retorna os resultados como um dicionário indexado por um campo.
        
        Args:
            query: Consulta SQL
            params: Parâmetros para a consulta
            key_field: Nome do campo a ser usado como chave
            
        Returns:
            Dicionário de resultados indexado pelo campo especificado
            
        Raises:
            KeyError: Se o campo chave não for encontrado nos resultados
            QueryError: Se ocorrer erro na execução da consulta
        """
        try:
            results, _ = self.execute(query, params)
            
            if not results:
                return {}
            
            # Verifica se o campo chave existe
            if key_field not in results[0]:
                raise KeyError(f"Campo chave \"{key_field}\" não encontrado nos resultados")
            
            # Cria o dicionário
            return {row[key_field]: row for row in results}
            
        except QueryError:
            raise

        except KeyError:
            raise

        except Exception as e:
            error_message = f"Erro ao executar query_to_dict: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise QueryError(error_message) from e
    
    def query_to_df(
        self, 
        query: str, 
        params: Optional[Any] = None, 
        index_col: Optional[str] = None
    ) -> 'pd.DataFrame':
        """
        Executa uma consulta e retorna os resultados como um DataFrame do Pandas.
        
        Args:
            query: Consulta SQL a ser executada
            params: Parâmetros para a consulta (opcional)
            index_col: Nome da coluna a ser usada como índice do DataFrame (opcional)
            
        Returns:
            DataFrame do Pandas com os resultados da consulta
            
        Raises:
            QueryError: Se ocorrer erro na execução da consulta ou conversão para DataFrame
            ValueError: Se o índice especificado não for encontrado nos resultados
        """
        try:
            import pandas as pd
            
            # Executa a consulta
            results, _ = self.execute(query, params)
            
            if not results:
                # Retorna DataFrame vazio com as colunas corretas se não houver resultados
                return pd.DataFrame()
            
            # Cria o DataFrame a partir dos resultados
            df = pd.DataFrame(results)
            
            # Define o índice se especificado
            if index_col is not None:
                if index_col not in df.columns:
                    raise ValueError(f"Coluna de índice '{index_col}' não encontrada no DataFrame")
                df.set_index(index_col, inplace=True)
                
            return df
            
        except (QueryError, ValueError):
            raise
            
        except Exception as e:
            error_message = f"Erro ao executar query_to_df: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise QueryError(error_message) from e

    def query_single_value(
        self, 
        query: str, 
        params: Optional[Any] = None
    ) -> Any:
        """
        Executa uma consulta e retorna um único valor escalar.
        
        Args:
            query: Consulta SQL
            params: Parâmetros para a consulta
            
        Returns:
            Valor único ou None
            
        Raises:
            QueryError: Se ocorrer erro na execução da consulta
        """
        try:
            results, _ = self.execute(query, params, fetch_all=False)
            
            if not results or not results[0]:
                return None
                
            # Retorna o primeiro valor do primeiro resultado
            first_row = results[0]
            first_key = list(first_row.keys())[0]
            return first_row[first_key]
            
        except Exception as e:
            error_message = f"Erro ao executar query_single_value: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise QueryError(error_message) from e
    
    def reset_pool(self) -> None:
        """
        Reinicia o pool de conexões para limpar quaisquer resultados não lidos.
        
        Isso deve ser usado quando ocorrerem erros de 'Unread result found'.
        """
        Log.warning(
            "Reiniciando pool de conexões devido a possíveis resultados não lidos", 
            name='MySQLConnector'
        )
        
        # Fecha o pool existente (se houver)
        self.close()
        
        # Recria o pool
        self._initialize_pool()
        
        Log.info("Pool de conexões reiniciado com sucesso", name='MySQLConnector')

    def close(self) -> None:
        """Fecha o pool de conexões."""
        self._pool = None
        self._initialized = False
        Log.info('Conexões MySQL fechadas', name='MySQLConnector')
    
    @staticmethod
    def from_env(
        log_file: Optional[str] = None, 
        log_level: LogLevel = LogLevel.INFO
    ) -> 'MySQLConnector':
        """
        Cria um conector a partir de variáveis de ambiente.
        
        Args:
            log_file: Caminho para arquivo de log
            log_level: Nível de log
            
        Returns:
            Instância de MySQLConnector
        """
        return MySQLConnector(log_file=log_file, log_level=log_level)
    
    @staticmethod
    def load_query_from_file(file_path: str) -> str:
        """
        Carrega uma consulta SQL de um arquivo.
        
        Args:
            file_path: Caminho para o arquivo SQL
            
        Returns:
            Consulta SQL como string
            
        Raises:
            FileNotFoundError: Se o arquivo não for encontrado
            IOError: Se ocorrer erro na leitura do arquivo
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                query = file.read()
            
            return query
            
        except Exception as e:
            error_message = f"Erro ao ler arquivo SQL {file_path}: {str(e)}"
            Log.error(error_message, name='MySQLConnector')
            raise