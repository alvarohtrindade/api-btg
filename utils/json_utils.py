"""
File: json_utils.py
Author: Cesar Godoy
Date: 2025-04-17
Version: 2.0
Description: Utilitários para carregamento de arquivos JSON e validação de schema de DataFrames.
"""

import json
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Generator, Set, Union
import pandas as pd
from pydantic import BaseModel, Field, validator
from utils.logging_utils import Log

logger = Log.get_logger(__name__)


class JsonErrorType(Enum):
    """Tipos de erros que podem ocorrer durante processamento de JSON."""
    FILE_NOT_FOUND = 'file_not_found'
    PARSE_ERROR = 'parse_error'
    PERMISSION_ERROR = 'permission_error'
    VALIDATION_ERROR = 'validation_error'
    UNEXPECTED_ERROR = 'unexpected_error'


class InvalidJsonError(Exception):
    """Exceção lançada quando o conteúdo de um JSON válido não atende aos requisitos esperados."""
    
    def __init__(self, message: str, error_type: JsonErrorType = JsonErrorType.VALIDATION_ERROR):
        super().__init__(message)
        self.error_type = error_type


class ColumnConfig(BaseModel):
    """Configuração de uma coluna no schema."""
    name: str
    type: str = 'string'
    required: bool = True
    description: Optional[str] = None


class DbConfig(BaseModel):
    """Configuração do banco de dados."""
    columns: List[ColumnConfig]
    table_name: Optional[str] = None


class ConfigModel(BaseModel):
    """Modelo completo de configuração."""
    db_config: DbConfig
    target_columns: Optional[List[str]] = None
    
    @validator('db_config')
    def validate_db_config(cls, v):
        if not v.columns:
            raise ValueError('A lista de colunas não pode estar vazia')
        return v


def load_execution_plan(plan_path: str) -> Dict[str, Any]:
    """
    Carrega o JSON de mapeamento de execução entre empresas e suas configurações.
    Implementa cache para evitar recarregamentos desnecessários.

    Args:
        plan_path: Caminho para o arquivo JSON do plano de execução.

    Returns:
        Dicionário contendo os mapeamentos de execução.
        
    Raises:
        FileNotFoundError: Se o arquivo não for encontrado.
        json.JSONDecodeError: Se o arquivo não for um JSON válido.
    """
    try:
        with open(plan_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    except FileNotFoundError:
        logger.error(
            'Arquivo de plano de execução não encontrado', 
            extra={'path': plan_path, 'error_type': JsonErrorType.FILE_NOT_FOUND.value}
        )
        raise

    except json.JSONDecodeError as e:
        logger.error(
            'Erro de parsing no JSON de plano de execução', 
            extra={
                'path': plan_path, 
                'error_type': JsonErrorType.PARSE_ERROR.value,
                'line': e.lineno,
                'position': e.colno
            }
        )
        raise

    except Exception as e:
        logger.error(
            'Erro inesperado ao carregar plano de execução', 
            extra={'path': plan_path, 'error_type': JsonErrorType.UNEXPECTED_ERROR.value}
        )
        raise


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Carrega o arquivo JSON de configuração da empresa com validação de estrutura.
    Implementa cache para evitar recarregamentos desnecessários.

    Args:
        config_path: Caminho para o JSON de configuração.

    Returns:
        Dicionário com a configuração carregada e validada.

    Raises:
        InvalidJsonError: Se o conteúdo do JSON não atender à estrutura esperada.
        FileNotFoundError: Se o arquivo não for encontrado.
        json.JSONDecodeError: Se o arquivo não for um JSON válido.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # Validação completa usando Pydantic
        config_model = ConfigModel(**config_data)
        return config_model.dict()

    except FileNotFoundError:
        logger.error(
            'Arquivo de configuração não encontrado', 
            extra={'path': config_path, 'error_type': JsonErrorType.FILE_NOT_FOUND.value}
        )
        raise

    except json.JSONDecodeError as e:
        logger.error(
            'Erro de parsing no JSON de configuração', 
            extra={
                'path': config_path, 
                'error_type': JsonErrorType.PARSE_ERROR.value,
                'line': e.lineno,
                'position': e.colno
            }
        )
        raise

    except PermissionError:
        logger.error(
            'Permissão negada ao acessar arquivo', 
            extra={'path': config_path, 'error_type': JsonErrorType.PERMISSION_ERROR.value}
        )
        raise
        
    except (ValueError, TypeError) as e:
        error_msg = f"Configuração inválida em '{config_path}': {str(e)}"
        logger.error(
            'Validação de estrutura falhou', 
            extra={'path': config_path, 'error_type': JsonErrorType.VALIDATION_ERROR.value}
        )
        raise InvalidJsonError(error_msg)

    except Exception as e:
        logger.error(
            'Erro inesperado ao carregar configuração', 
            extra={'path': config_path, 'error_type': JsonErrorType.UNEXPECTED_ERROR.value}
        )
        raise


def extract_column_specs(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Extrai as especificações de colunas da configuração.
    
    Args:
        config: Dicionário de configuração.
        
    Returns:
        Dicionário com especificações de colunas.
    """
    target_columns = config.get('target_columns')
    if target_columns:
        # Se target_columns é uma lista de strings, converter para dicionário
        if isinstance(target_columns, list) and all(isinstance(col, str) for col in target_columns):
            return {col: {'name': col, 'required': True} for col in target_columns}
        return target_columns
        
    return {
        col['name']: col for col in config.get('db_config', {}).get('columns', [])
        if isinstance(col, dict) and 'name' in col
    }


def _check_column_type(series: pd.Series, expected_type: str) -> bool:
    """
    Verifica se o tipo de dados de uma coluna é compatível com o esperado.
    
    Args:
        series: Série do pandas representando uma coluna.
        expected_type: Tipo esperado ('string', 'integer', 'float', 'boolean', 'date').
        
    Returns:
        True se compatível, False caso contrário.
    """
    if expected_type == 'string':
        return series.dtype == 'object' or pd.api.types.is_string_dtype(series)
    
    elif expected_type == 'integer':
        return pd.api.types.is_integer_dtype(series)
    
    elif expected_type == 'float':
        return pd.api.types.is_float_dtype(series)
    
    elif expected_type == 'boolean':
        return pd.api.types.is_bool_dtype(series)
    
    elif expected_type == 'date':
        return pd.api.types.is_datetime64_dtype(series)
    
    return True  # Para tipos desconhecidos, aceitar por padrão


def iter_validation_errors(df: pd.DataFrame, config: Dict[str, Any]) -> Generator[str, None, None]:
    """
    Gera erros de validação de forma eficiente para grandes DataFrames.
    
    Args:
        df: DataFrame a ser validado.
        config: Configuração com schema.
        
    Yields:
        Mensagens de erro encontradas durante a validação.
    """
    try:
        column_specs = extract_column_specs(config)
        
        # Verificar presença das colunas
        for col_name, col_specs in column_specs.items():
            required = col_specs.get('required', True)
            
            if col_name not in df.columns:
                if required:
                    yield f"Coluna obrigatória ausente: '{col_name}'"
                continue
                
            # Verificar tipos de dados se especificado
            col_type = col_specs.get('type')
            if col_type and not _check_column_type(df[col_name], col_type):
                yield f"Tipo de dados incompatível na coluna '{col_name}'. Esperado: {col_type}"
                
    except Exception as e:
        logger.error(
            'Erro durante a iteração de validação', 
            extra={'error': str(e), 'error_type': JsonErrorType.UNEXPECTED_ERROR.value}
        )
        yield f"Erro interno durante validação: {str(e)}"


def validate_schema(df: pd.DataFrame, config: Dict[str, Any]) -> List[str]:
    """
    Valida se todas as colunas obrigatórias do schema estão presentes no DataFrame
    e se os tipos de dados são compatíveis.
    
    Args:
        df: DataFrame a ser validado.
        config: Dicionário de configuração contendo o schema.
        
    Returns:
        Lista de mensagens de erro, vazia se não houver problemas.
    """
    try:
        return list(iter_validation_errors(df, config))
        
    except Exception as e:
        logger.error(
            'Erro durante a validação do schema', 
            extra={'error': str(e), 'error_type': JsonErrorType.UNEXPECTED_ERROR.value}
        )
        return [f"Erro interno durante validação: {str(e)}"]


class ConfigValidator:
    """Classe para validação e manipulação de configurações."""
    
    def __init__(self, config_path: str):
        """
        Inicializa o validador com o caminho da configuração.
        
        Args:
            config_path: Caminho para o arquivo de configuração JSON.
        """
        self._config_path = config_path
        self._config = None
        self._columns_cache = None
        
    @property
    def config(self) -> Dict[str, Any]:
        """
        Obtém a configuração carregada.
        
        Returns:
            Dicionário de configuração.
        """
        if self._config is None:
            self._config = load_config(self._config_path)
        return self._config
    
    @property
    def columns(self) -> Dict[str, Dict[str, Any]]:
        """
        Obtém as especificações de colunas da configuração.
        
        Returns:
            Dicionário com especificações de colunas.
        """
        if self._columns_cache is None:
            self._columns_cache = extract_column_specs(self.config)
        return self._columns_cache
    
    def validate_dataframe(self, df: pd.DataFrame) -> List[str]:
        """
        Valida um DataFrame contra a configuração carregada.
        
        Args:
            df: DataFrame a ser validado.
            
        Returns:
            Lista de erros de validação, vazia se não houver problemas.
        """
        return validate_schema(df, self.config)
    
    def get_required_columns(self) -> Set[str]:
        """
        Obtém o conjunto de colunas obrigatórias.
        
        Returns:
            Conjunto de nomes de colunas obrigatórias.
        """
        return {
            col_name for col_name, specs in self.columns.items()
            if specs.get('required', True)
        }
    
def parse_metrics_from_output(output: str) -> Dict[str, Any]:
    """
    Extrai um JSON de métricas de dentro de uma string (stdout) e retorna um dicionário.
    Procura pela primeira ocorrência de chaves { ... } e tenta fazer json.loads.

    Se não encontrar um JSON válido, retorna um dicionário vazio.
    """
    # Regex para pegar o objeto JSON (assumindo que comece em '{' e termine em '}')
    match = re.search(r'\{.*\}', output, flags=re.DOTALL)
    if not match:
        return {}

    texto_json = match.group(0)
    try:
        return json.loads(texto_json)
    except json.JSONDecodeError:
        return {}