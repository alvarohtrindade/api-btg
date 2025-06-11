"""
File: mysql_table_utils.py
Author: Cesar Godoy
Date: 2025-04-08
Version: 1.0
Description: Utilitário para operações com tabelas MySQL,
             implementa métodos de consulta, verificação e alteração
             de estruturas de tabelas.
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, Set
from datetime import datetime
from contextlib import contextmanager

# Módulos internos
from utils.logging_utils import Log, LogLevel
from utils.mysql_connector_utils import MySQLConnector, MySQLError, QueryError


class TableError(MySQLError):
    """Exceção para erros relacionados a operações com tabelas."""
    pass


class MySQLTableManager:
    """
    Gerenciador de tabelas MySQL com suporte a operações de consulta,
    verificação, manutenção e modificação de tabelas.
    """
    
    def __init__(
        self,
        connector: MySQLConnector
    ):
        """
        Inicializa o gerenciador de tabelas MySQL.
        
        Args:
            connector: Instância de MySQLConnector para conexão com o banco
        """
        self.connector = connector
        self.database = connector.config.database
        
        Log.info(
            f"MySQLTableManager inicializado para database {self.database}", 
            name='MySQLTableManager'
        )
    
    #
    # Métodos de informação sobre tabelas
    #
    def get_tables(self) -> List[str]:
        """
        Retorna a lista de todas as tabelas no banco de dados atual.
        
        Returns:
            Lista com os nomes das tabelas
            
        Raises:
            TableError: Em caso de erro na execução da query
        """
        try:
            query = "SHOW TABLES"
            results = self.connector.execute_query(query)
            
            # O resultado de SHOW TABLES tem apenas uma coluna com o nome da tabela
            # O nome da coluna é variável, então pegamos o primeiro valor de cada linha
            tables = [list(row.values())[0] for row in results]
            
            Log.debug(
                f"Encontradas {len(tables)} tabelas no banco {self.database}", 
                name='MySQLTableManager'
            )

            return tables
            
        except Exception as e:
            error_message = f"Erro ao obter lista de tabelas: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def table_exists(self, table_name: str) -> bool:
        """
        Verifica se uma tabela existe no banco de dados.
        
        Args:
            table_name: Nome da tabela a ser verificada
            
        Returns:
            True se a tabela existir, False caso contrário
        """
        try:
            query = """
            SELECT COUNT(*) AS count 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
            """
            count = self.connector.query_single_value(query, (self.database, table_name))
            return count > 0
            
        except Exception as e:
            Log.warning(
                f"Erro ao verificar existência da tabela {table_name}: {str(e)}", 
                name='MySQLTableManager'
            )
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Obtém informações detalhadas sobre uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Dicionário com informações da tabela
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            # Obter informações básicas da tabela
            query = """
            SELECT 
                table_name,
                engine,
                table_rows,
                avg_row_length,
                data_length,
                index_length,
                auto_increment,
                table_collation,
                create_time,
                update_time,
                table_comment
            FROM information_schema.tables
            WHERE 
                table_schema = %s AND 
                table_name = %s
            """
            results = self.connector.execute_query(query, (self.database, table_name))
            
            if not results:
                raise TableError(f"Não foi possível obter informações da tabela {table_name}")
                
            table_info = results[0]
            
            # Adicionar contagem real de linhas (mais precisa que table_rows)
            count_query = f"SELECT COUNT(*) AS row_count FROM `{table_name}`"
            try:
                row_count = self.connector.query_single_value(count_query)
                table_info['row_count'] = row_count

            except Exception as e:
                Log.warning(
                    f"Erro ao contar linhas da tabela {table_name}: {str(e)}", 
                    name='MySQLTableManager'
                )
                table_info['row_count'] = table_info['table_rows']
            
            # Converter alguns campos para formatos mais amigáveis
            if table_info.get('create_time'):
                table_info['create_time'] = table_info['create_time'].isoformat() if isinstance(table_info['create_time'], datetime) else table_info['create_time']
                
            if table_info.get('update_time'):
                table_info['update_time'] = table_info['update_time'].isoformat() if isinstance(table_info['update_time'], datetime) else table_info['update_time']
                
            # Calcular tamanho total
            table_info['total_size'] = (table_info.get('data_length') or 0) + (table_info.get('index_length') or 0)
            
            Log.debug(
                f"Informações da tabela {table_name} obtidas com sucesso", 
                name='MySQLTableManager'
            )
            return table_info
            
        except QueryError as e:
            raise TableError(f"Erro ao obter informações da tabela {table_name}: {str(e)}") from e
        
        except Exception as e:
            error_message = f"Erro ao obter informações da tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtém informações sobre as colunas de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Lista de dicionários com informações das colunas
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = """
            SELECT 
                column_name,
                column_type,
                is_nullable,
                column_key,
                column_default,
                extra,
                character_set_name,
                collation_name,
                column_comment,
                ordinal_position
            FROM information_schema.columns
            WHERE 
                table_schema = %s AND
                table_name = %s
            ORDER BY ordinal_position
            """
            columns = self.connector.execute_query(query, (self.database, table_name))
            
            Log.debug(
                f"Obtidas informações de {len(columns)} colunas da tabela {table_name}", 
                name='MySQLTableManager'
            )
        
            return columns
            
        except Exception as e:
            error_message = f"Erro ao obter colunas da tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtém informações sobre os índices de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Lista de dicionários com informações dos índices
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = """
            SELECT 
                index_name,
                non_unique,
                seq_in_index,
                column_name,
                collation,
                cardinality,
                sub_part,
                index_type,
                comment
            FROM information_schema.statistics
            WHERE 
                table_schema = %s AND 
                table_name = %s
            ORDER BY 
                index_name, 
                seq_in_index
            """
            results = self.connector.execute_query(query, (self.database, table_name))
            
            # Criar versões case-insensitive dos resultados
            results_ci = []
            for row in results:
                row_ci = {k.lower(): v for k, v in row.items()}
                results_ci.append(row_ci)
            
            # Agrupar as colunas do mesmo índice
            indexes = {}
            for row in results_ci:
                index_name = row['index_name']
                if index_name not in indexes:
                    indexes[index_name] = {
                        'index_name': index_name,
                        'non_unique': row['non_unique'],
                        'index_type': row['index_type'],
                        'comment': row['comment'],
                        'columns': []
                    }
                
                indexes[index_name]['columns'].append({
                    'column_name': row['column_name'],
                    'seq_in_index': row['seq_in_index'],
                    'collation': row['collation'],
                    'sub_part': row['sub_part']
                })
            
            Log.debug(
                "Obtidas informações de {len(indexes)} índices da tabela {table_name}", 
                name='MySQLTableManager'
            )

            return list(indexes.values())
            
        except Exception as e:
            error_message = f"Erro ao obter índices da tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
        
    
    def get_decimal_column_precisions(self, table_name: str) -> Dict[str, int]:
        """
        Retorna a precisão decimal (número de casas decimais) para colunas DECIMAL/NUMERIC.

        Args:
            table_name: Nome da tabela

        Returns:
            Dicionário onde as chaves são os nomes das colunas e os valores são as precisões decimais
        """
        try:
            query = """
            SELECT 
                column_name, 
                numeric_scale
            FROM information_schema.columns
            WHERE 
                table_schema = %s AND 
                table_name = %s AND 
                data_type IN ('decimal', 'numeric')
            """
            results = self.connector.execute_query(query, (self.database, table_name))

            precisions = {
                row['column_name']: int(row['numeric_scale']) if row['numeric_scale'] is not None else 0
                for row in results
            }

            Log.debug(
                f"Precisões decimais obtidas da tabela {table_name}: {precisions}", 
                name='MySQLTableManager'
            )

            return precisions

        except Exception as e:
            Log.warning(f"Erro ao obter precisões decimais da tabela {table_name}: {e}", name='MySQLTableManager')
            return {}


    def get_create_table(self, table_name: str) -> str:
        """
        Obtém o comando CREATE TABLE da tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            String com o comando CREATE TABLE
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"SHOW CREATE TABLE `{table_name}`"
            results = self.connector.execute_query(query)
            
            if not results or 'Create Table' not in results[0]:
                raise TableError(f"Não foi possível obter o comando CREATE TABLE para {table_name}")
                
            return results[0]['Create Table']
            
        except Exception as e:
            error_message = f"Erro ao obter comando CREATE TABLE para {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    #
    # Métodos de manutenção de tabelas
    #
    def check_table(self, table_name: str) -> Dict[str, Any]:
        """
        Verifica a integridade de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Dicionário com resultado da verificação
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"CHECK TABLE `{table_name}`"
            # Use o método execute() fornecendo os parâmetros corretos
            results, _ = self.connector.execute(query, None, True)
            
            Log.debug(
                f"Verificação da tabela {table_name} realizada: {results}", 
                name='MySQLTableManager'
            )
            return results[0] if results else {}
            
        except Exception as e:
            error_message = f"Erro ao verificar tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def optimize_table(self, table_name: str) -> Dict[str, Any]:
        """
        Otimiza uma tabela para melhorar o desempenho.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Dicionário com resultado da otimização
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"OPTIMIZE TABLE `{table_name}`"
            results = self.connector.execute_query(query)
            
            Log.info(
                f"Otimização da tabela {table_name} realizada", 
                name='MySQLTableManager'
            )
            return results[0] if results else {}
            
        except Exception as e:
            error_message = f"Erro ao otimizar tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def repair_table(self, table_name: str) -> Dict[str, Any]:
        """
        Repara uma tabela corrompida.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Dicionário com resultado do reparo
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"REPAIR TABLE `{table_name}`"
            results = self.connector.execute_query(query)
            
            Log.info(
                f"Reparo da tabela {table_name} realizado", 
                name='MySQLTableManager'
            )

            return results[0] if results else {}
            
        except Exception as e:
            error_message = f"Erro ao reparar tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """
        Analisa e armazena a distribuição de chaves de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Dicionário com resultado da análise
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"ANALYZE TABLE `{table_name}`"
            results = self.connector.execute_query(query)
            
            Log.info(
                f"Análise da tabela {table_name} realizada", 
                name='MySQLTableManager'
            )

            return results[0] if results else {}
            
        except Exception as e:
            error_message = f"Erro ao analisar tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Remove todos os dados de uma tabela sem remover a estrutura.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            True se a operação for bem-sucedida
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"TRUNCATE TABLE `{table_name}`"
            self.connector.execute_update(query)
            
            Log.info(
                f"Tabela {table_name} truncada com sucesso", 
                name='MySQLTableManager'
            )
            return True
            
        except Exception as e:
            error_message = f"Erro ao truncar tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    #
    # Métodos de criação e alteração de tabelas
    #
    def create_table_like(
        self, 
        new_table_name: str, 
        source_table_name: str, 
        with_data: bool = False
    ) -> bool:
        """
        Cria uma nova tabela com a mesma estrutura de uma tabela existente.
        
        Args:
            new_table_name: Nome da nova tabela
            source_table_name: Nome da tabela de origem
            with_data: Se True, copia também os dados
            
        Returns:
            True se a operação for bem-sucedida
            
        Raises:
            TableError: Se a tabela de origem não existir ou ocorrer outro erro
        """
        if not self.table_exists(source_table_name):
            error_message = f"Tabela de origem {source_table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            # Verifica se a tabela de destino já existe
            if self.table_exists(new_table_name):
                Log.warning(
                    f"Tabela de destino {new_table_name} já existe. Nenhuma ação realizada.", 
                    name='MySQLTableManager'
                )

                return False
            
            # Cria a tabela com a mesma estrutura
            if with_data:
                query = f"CREATE TABLE `{new_table_name}` AS SELECT * FROM `{source_table_name}`"

            else:
                query = f"CREATE TABLE `{new_table_name}` LIKE `{source_table_name}`"
                
            self.connector.execute_update(query)
            
            Log.info(
                f"Tabela {new_table_name} criada com base em {source_table_name} "
                f"{"com" if with_data else "sem"} dados", 
                name='MySQLTableManager'
            )
            return True
            
        except Exception as e:
            error_message = f"Erro ao criar tabela {new_table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def rename_table(self, old_table_name: str, new_table_name: str) -> bool:
        """
        Renomeia uma tabela.
        
        Args:
            old_table_name: Nome atual da tabela
            new_table_name: Novo nome para a tabela
            
        Returns:
            True se a operação for bem-sucedida
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(old_table_name):
            error_message = f"Tabela {old_table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            # Verifica se a nova tabela já existe
            if self.table_exists(new_table_name):
                error_message = f"Tabela de destino {new_table_name} já existe"
                Log.error(error_message, name='MySQLTableManager')
                raise TableError(error_message)
            
            query = f"RENAME TABLE `{old_table_name}` TO `{new_table_name}`"
            self.connector.execute_update(query)
            
            Log.info(
                f"Tabela {old_table_name} renomeada para {new_table_name}", 
                name='MySQLTableManager'
            )

            return True
            
        except TableError:
            raise

        except Exception as e:
            error_message = f"Erro ao renomear tabela {old_table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """
        Remove uma tabela do banco de dados.
        
        Args:
            table_name: Nome da tabela
            if_exists: Se True, não gera erro se a tabela não existir
            
        Returns:
            True se a operação for bem-sucedida
            
        Raises:
            TableError: Se a tabela não existir (quando if_exists=False) ou ocorrer outro erro
        """
        # Verifica se a tabela existe apenas quando if_exists=False
        if not if_exists and not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            if_exists_clause = "IF EXISTS " if if_exists else ""
            query = f"DROP TABLE {if_exists_clause}`{table_name}`"
            self.connector.execute_update(query)
            
            Log.info(
                f"Tabela {table_name} removida com sucesso", 
                name='MySQLTableManager'
            )

            return True
            
        except Exception as e:
            error_message = f"Erro ao remover tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    def get_table_status(self, table_name: str) -> Dict[str, Any]:
        """
        Obtém informações detalhadas de status de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Dicionário com informações de status da tabela
            
        Raises:
            TableError: Se a tabela não existir ou ocorrer outro erro
        """
        if not self.table_exists(table_name):
            error_message = f"Tabela {table_name} não existe"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message)
            
        try:
            query = f"SHOW TABLE STATUS LIKE '{table_name}'"
            results = self.connector.execute_query(query)
            
            if not results:
                raise TableError(f"Não foi possível obter status da tabela {table_name}")
                
            return results[0]
            
        except Exception as e:
            error_message = f"Erro ao obter status da tabela {table_name}: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e
    
    #
    # Métodos para verificar suporte a recursos
    #
    def engine_supports_partitioning(self, engine: Optional[str] = None) -> bool:
        """
        Verifica se o MySQL suporta particionamento e, se um engine for informado,
        se este engine suporta particionamento.

        Caso a variável 'have_partitioning' não seja encontrada (ex.: em Aurora MySQL),
        o método verifica a existência de 'aurora_version' e assume que o particionamento
        está disponível.

        Args:
            engine (Optional[str]): Nome do mecanismo de armazenamento (ex.: 'innodb', 'myisam')

        Returns:
            bool: True se o servidor (e o engine, se especificado) suportar particionamento,
                False caso contrário.
        """
        try:
            # Primeira tentativa: verificar suporte global a particionamento.
            query = "SHOW VARIABLES LIKE 'have_partitioning'"
            result = self.connector.query_single_value(query)

            # Se não houver variável 'have_partitioning', pode ser um Aurora MySQL.
            if result is None:
                query = "SHOW VARIABLES LIKE 'aurora_version'"
                aurora_version = self.connector.query_single_value(query)
                if aurora_version is not None:
                    # Se a variável 'aurora_version' existir, consideramos que o particionamento é suportado.
                    result = "YES"

            if not result or result.strip().upper() != "YES":
                Log.debug(
                    "O servidor MySQL não indica suporte global a particionamento",
                    name="MySQLTableManager"
                )
                return False

            # Se um engine específico foi informado, valida se ele é conhecido por suportar particionamento.
            if engine:
                engine = engine.lower()
                supported_engines = {"innodb", "myisam", "ndbcluster", "archive"}
                if engine not in supported_engines:
                    Log.debug(
                        f"Engine {engine} não suporta particionamento",
                        name="MySQLTableManager"
                    )
                    return False

            Log.debug(
                f"Suporte a particionamento ativo para engine {engine if engine else 'geral'}",
                name="MySQLTableManager"
            )
            return True

        except Exception as e:
            Log.warning(
                f"Erro ao verificar suporte a particionamento: {e}",
                name="MySQLTableManager"
            )
            return False

    
    def get_supported_engines(self) -> List[Dict[str, Any]]:
        """
        Obtém a lista de engines de armazenamento suportados pelo servidor.

        Returns:
            List[Dict[str, Any]]: Lista de dicionários contendo informações sobre os engines.

        Raises:
            TableError: Se ocorrer algum erro ao executar a consulta.
        """
        query = "SHOW ENGINES"
        try:
            results = self.connector.execute_query(query)
            
            # Caso o resultado seja None, assegura que retornaremos uma lista.
            if results is None:
                results = []

            Log.debug(
                f"Obtidos {len(results)} engines suportados",
                name="MySQLTableManager"
            )
            return results

        except Exception as e:
            error_message = f"Erro ao obter engines suportados: {e}"
            Log.error(error_message, name="MySQLTableManager")
            raise TableError(error_message) from e

    

    # Método para debugging
    def execute_raw_query(
        self, 
        query: str, 
        params: Optional[Union[Dict[str, Any], List[Any], Tuple[Any, ...]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SQL arbitrária para debugging ou operações especiais.
        ATENÇÃO: Use com cuidado!
        
        Args:
            query: Consulta SQL
            params: Parâmetros para a consulta
            
        Returns:
            Resultados da consulta
            
        Raises:
            TableError: Se ocorrer erro na execução da consulta
        """
        try:
            Log.warning(f"Executando consulta arbitrária: {query}", name='MySQLTableManager')
            return self.connector.execute_query(query, params)
            
        except Exception as e:
            error_message = f"Erro ao executar consulta arbitrária: {str(e)}"
            Log.error(error_message, name='MySQLTableManager')
            raise TableError(error_message) from e

