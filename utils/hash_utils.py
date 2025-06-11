"""
File: hash_utils.py
Author: Cesar Godoy
Date: 2025-04-17
Version: 1.1
Description: Funções auxiliares para gerar hashes de arquivos e registros.
             Inclui suporte a normalização de casas decimais com base na precisão
             definida para colunas específicas de tabelas MySQL.
"""

import os
import json
import hashlib
import pandas as pd
import numpy as np
import mimetypes

from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Dict
from utils.logging_utils import Log
from concurrent.futures import ThreadPoolExecutor

logger = Log.get_logger(__name__)


def normalize_decimal_columns(df: pd.DataFrame, column_precisions: Dict[str, int]) -> pd.DataFrame:
    """
    Arredonda colunas decimais com base na precisão definida para garantir consistência de hash.

    Args:
        df: DataFrame de entrada
        column_precisions: Dicionário com nome da coluna e precisão desejada

    Returns:
        DataFrame com colunas decimais normalizadas
    """
    for col, precision in column_precisions.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda x: round(x, precision) if pd.notnull(x) else x)
    return df


def generate_row_hash(row_data: Any) -> str:
    """
    Gera um hash MD5 para uma linha de dados.

    Args:
        row_data: Linha como dicionário ou pandas Series.

    Returns:
        Hash MD5 da linha ou hash zerado em caso de erro.
    """
    try:
        row_dict = {}

        for key, value in row_data.items():
            if pd.isna(value) or value is None:
                row_dict[key] = 'null'

            elif isinstance(value, pd.Timestamp):
                row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')

            elif isinstance(value, (np.generic,)):
                row_dict[key] = str(value.item())

            elif isinstance(value, (str, int, float, bool)):
                row_dict[key] = str(value)

            else:
                row_dict[key] = json.dumps(value, default=str)

        data_str = ''.join(f"{key}:{row_dict[key]}" for key in sorted(row_dict.keys()))
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    except Exception as e:
        logger.error(f"Erro ao gerar hash da linha: {e}")
        return '0' * 32


def process_dataframe(df: pd.DataFrame, column_precisions: Dict[str, int]) -> pd.Series:
    """
    Aplica hashing linha a linha em um DataFrame após normalização de colunas decimais.

    Args:
        df: DataFrame a ser processado.
        column_precisions: Dicionário com precisão por coluna decimal

    Returns:
        Série com os hashes por linha.
    """
    try:
        df = normalize_decimal_columns(df.copy(), column_precisions)

        if len(df) < 10_000:
            return df.apply(generate_row_hash, axis=1)
        else:
            return pd.Series(process_large_dataframe(df), index=df.index)

    except Exception as e:
        logger.error(f"Erro ao processar DataFrame para hashing: {e}")
        return pd.Series(['0' * 32] * len(df), index=df.index)


def process_large_dataframe(df: pd.DataFrame, batch_size: int = 1000) -> Generator[str, None, None]:
    """
    Processa DataFrame em lotes com paralelismo para hashing eficiente.

    Args:
        df: DataFrame grande.
        batch_size: Tamanho de lote por iteração.

    Yields:
        Hashes por linha.
    """
    try:
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            with ThreadPoolExecutor() as executor:
                for hash_value in executor.map(generate_row_hash, batch.to_dict(orient='records')):
                    yield hash_value

    except Exception as e:
        logger.error(f"Erro ao processar lote de DataFrame: {e}")
        yield from ['0' * 32] * len(df)


def generate_file_hash(filepath: str) -> str:
    """
    Gera um hash MD5 para o conteúdo de um arquivo.

    Args:
        filepath: Caminho do arquivo.

    Returns:
        Hash MD5 do conteúdo ou hash zerado em caso de erro.
    """
    try:
        size = os.path.getsize(filepath)
        if size < 10 * 1024 * 1024:  # 10MB
            return generate_file_hash_small(filepath)
        
        else:
            return generate_file_hash_large(filepath)

    except Exception as e:
        logger.error(f"Erro ao calcular hash do arquivo {filepath}: {e}")
        return '0' * 32


def generate_file_hash_small(filepath: str) -> str:
    """
    Gera hash para arquivos pequenos carregando todo o conteúdo em memória.

    Args:
        filepath: Caminho do arquivo.

    Returns:
        Hash MD5 do conteúdo.
    """
    try:
        with open(filepath, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception as e:
        logger.error(f"Erro ao calcular hash de arquivo pequeno {filepath}: {e}")
        return '0' * 32


def generate_file_hash_large(filepath: str) -> str:
    """
    Gera hash para arquivos grandes usando processamento em chunks.

    Args:
        filepath: Caminho do arquivo.

    Returns:
        Hash MD5 do conteúdo.
    """
    try:
        md5_hash = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)

        return md5_hash.hexdigest()
    
    except Exception as e:
        logger.error(f"Erro ao calcular hash de arquivo grande {filepath}: {e}")
        return '0' * 32


def calculate_file_fingerprint(input_file: str) -> Dict[str, str]:
    """
    Gera um fingerprint do arquivo para rastreabilidade e controle de reprocessamento.

    Args:
        input_file: Caminho absoluto ou relativo do arquivo.

    Returns:
        Dicionário com hash, metadados e timestamp de processamento.
    """
    file_path = Path(input_file)

    try:
        file_hash = generate_file_hash(str(file_path))
        file_stat = file_path.stat()
        mime_type, _ = mimetypes.guess_type(file_path)

        return {
            'filename': file_path.name,
            'path': str(file_path.resolve()),
            'size_bytes': file_stat.st_size,
            'modification_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            'creation_time': datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
            'mime_type': mime_type or 'unknown',
            'file_hash': file_hash,
            'processed_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.warning(f"Erro ao calcular fingerprint do arquivo '{input_file}': {e}")
        return {
            'filename': file_path.name,
            'path': str(file_path.resolve()),
            'file_hash': '0' * 32,
            'size_bytes': None,
            'mime_type': 'unknown',
            'creation_time': None,
            'modification_time': None,
            'processed_at': datetime.now().isoformat(),
            'error': str(e)
        }