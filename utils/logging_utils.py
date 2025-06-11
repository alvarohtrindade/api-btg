"""
File: logging_utils.py
Author: Cesar Godoy
Date: 2025-04-05
Version: 1.0
Description: Classe de logging thread-safe com filtragem baseada em severidade,
             saída configurável para console e/ou arquivo, e timestamps formatados.
             Suporta rotação de logs, logging estruturado e contextos por thread.
"""

import sys
import os
import inspect
import traceback
import atexit
import threading
import logging
import json
import time
from typing import Any, Optional, TextIO, Dict, List, Union, ContextManager, TypeVar, cast
from enum import IntEnum
from datetime import datetime
from logging.handlers import RotatingFileHandler, QueueHandler
from queue import Queue
import logging.config
from contextlib import contextmanager


class LogLevel(IntEnum):
    """
    Níveis de severidade de log, alinhados com os níveis do módulo logging padrão.
    """
    DEBUG = logging.DEBUG         # 10
    INFO = logging.INFO           # 20
    WARNING = logging.WARNING     # 30
    ERROR = logging.ERROR         # 40
    CRITICAL = logging.CRITICAL   # 50
    NONE = logging.CRITICAL + 10  # 60 - Desativa todos os logs


class LogColors:
    """
    Códigos ANSI para colorir logs no console.
    """
    DEBUG =    '\033[94m'    # Azul
    INFO =     '\033[92m'    # Verde
    WARNING =  '\033[93m'    # Amarelo
    ERROR =    '\033[91m'    # Vermelho
    CRITICAL = '\033[91;1m'  # Vermelho brilhante
    RESET =     '\033[0m'    # Reset


class Log:
    """
    Classe singleton para gerenciamento centralizado de logs.
    Fornece interface thread-safe, formatação flexível e múltiplos destinos.
    """
    # Implementação Singleton
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Log, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    # Atributos de instância
    def _initialize(self) -> None:
        """Inicializa o singleton com valores padrão."""
        self._level = LogLevel.DEBUG
        self._log_file: Optional[TextIO] = None
        self._log_file_path: Optional[str] = None
        self._use_console = True
        self._use_colors = True
        self._lock = threading.Lock()
        self._max_file_size_bytes: Optional[int] = None
        self._current_file_size: int = 0
        self._initialized = False
        
        # Atributos para integração com logging
        self._logging_configured = False
        self._format_string = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        self._file_handler = None
        self._console_handler = None
        
        # Atributos para logging assíncrono
        self._async_enabled = False
        self._queue: Optional[Queue] = None
        self._listener = None
        
        # Formato padrão para entradas de log
        self._LOG_FORMAT = '{timestamp} - {name} - {level} - [{filename}:{lineno}] - {message}'
    
    # Contexto por thread
    _context_data = threading.local()
    
    @staticmethod
    def _get_instance() -> 'Log':
        """
        Retorna a instância singleton da classe Log.
        
        Returns:
            Log: Instância singleton
        """
        return Log()
    
    @staticmethod
    def set_level(level: LogLevel) -> None:
        """
        Define o nível mínimo de severidade para logging.
        
        Args:
            level: Nível mínimo de severidade para registrar mensagens
        """
        instance = Log._get_instance()
        instance._level = level
        
        if instance._logging_configured:
            logging.getLogger().setLevel(level)
    
    @staticmethod
    def set_console_output(enabled: bool) -> None:
        """
        Habilita ou desabilita a saída de log para o console.
        
        Args:
            enabled: True para habilitar saída para console, False para desabilitar
        """
        instance = Log._get_instance()
        instance._use_console = enabled
        
        if instance._logging_configured and instance._console_handler:
            root_logger = logging.getLogger()
            if enabled and instance._console_handler not in root_logger.handlers:
                root_logger.addHandler(instance._console_handler)
                
            elif not enabled and instance._console_handler in root_logger.handlers:
                root_logger.removeHandler(instance._console_handler)
    
    @staticmethod
    def set_colored_output(enabled: bool = True) -> None:
        """
        Habilita ou desabilita saída colorida no console.
        
        Args:
            enabled: True para habilitar cores, False para desabilitar
        """
        instance = Log._get_instance()
        instance._use_colors = enabled
    
    @staticmethod
    def set_log_file(file_path: str, append: bool = False, max_size_mb: Optional[float] = 5.0) -> bool:
        """
        Configura o arquivo para saída de log com opção de rotação por tamanho.
        
        Args:
            file_path: Caminho para o arquivo de log
            append: Se True, anexa ao arquivo existente; se False, recria o arquivo
            max_size_mb: Tamanho máximo do arquivo em megabytes antes da rotação.
                         O padrão é 5.0 MB. Use None para desativar a rotação.
            
        Returns:
            bool: True se o arquivo foi aberto com sucesso, False caso contrário
        """
        instance = Log._get_instance()
        
        with instance._lock:
            if instance._log_file is not None:
                try: 
                    instance._log_file.close()
                except Exception: 
                    pass
                instance._log_file = None

            instance._log_file_path = file_path
            
            if max_size_mb is not None:
                instance._max_file_size_bytes = int(max_size_mb * 1024 * 1024)
            else:
                instance._max_file_size_bytes = None
            
            instance._current_file_size = 0
            if append and os.path.exists(file_path):
                try:
                    instance._current_file_size = os.path.getsize(file_path)
                except OSError:
                    instance._current_file_size = 0
            
            try:
                log_dir = os.path.dirname(file_path)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                
                if append and instance._max_file_size_bytes is not None and instance._current_file_size >= instance._max_file_size_bytes:
                    return instance._rotate_log_file()
                else:
                    mode = 'a' if append else 'w'
                    instance._log_file = open(file_path, mode, encoding='utf-8')
                    
                    if not instance._initialized:
                        atexit.register(instance._cleanup_on_exit)
                        instance._initialized = True
                    
                    instance._configure_logging_system(max_size_mb)
                    
                    return True
            
            except Exception as e:
                sys.stderr.write(f"Erro ao abrir arquivo de log {file_path}: {str(e)}\n")
                instance._log_file = None
                instance._log_file_path = None
                return False
    
    @staticmethod
    def configure_async_logging(enabled: bool = True, queue_size: int = 1000) -> None:
        """
        Configura logging assíncrono para minimizar o impacto no desempenho da aplicação.
        
        Args:
            enabled: True para ativar logging assíncrono
            queue_size: Tamanho máximo da fila de mensagens
        """
        instance = Log._get_instance()
        
        with instance._lock:
            # Se já estiver no estado desejado, não faz nada
            if instance._async_enabled == enabled:
                return
                
            # Se já está configurado, precisa reconfigurar
            if instance._logging_configured:
                # Desabilitar async logging
                if not enabled and instance._async_enabled:
                    if instance._listener:
                        instance._listener.stop()
                        instance._listener = None
                    
                    # Remover QueueHandlers
                    root_logger = logging.getLogger()
                    for handler in list(root_logger.handlers):
                        if isinstance(handler, QueueHandler):
                            root_logger.removeHandler(handler)
                    
                    # Reconfigurar loggers normais
                    instance._configure_logging_system()
                    instance._async_enabled = False
                
                # Habilitar async logging
                elif enabled and not instance._async_enabled:
                    # Criar fila e listener
                    instance._queue = Queue(maxsize=queue_size)
                    
                    # Configurar handlers para o listener
                    handlers = []
                    
                    if instance._console_handler:
                        handlers.append(instance._console_handler)
                    
                    if instance._file_handler:
                        handlers.append(instance._file_handler)
                    
                    # Importação local para evitar erros em versões antigas do Python
                    from logging.handlers import QueueListener
                    instance._listener = QueueListener(
                        instance._queue, 
                        *handlers,
                        respect_handler_level=True
                    )
                    instance._listener.start()
                    
                    # Remover handlers atuais
                    root_logger = logging.getLogger()
                    for handler in list(root_logger.handlers):
                        if not isinstance(handler, QueueHandler):
                            root_logger.removeHandler(handler)
                    
                    # Adicionar QueueHandler
                    queue_handler = QueueHandler(instance._queue)
                    root_logger.addHandler(queue_handler)
                    
                    instance._async_enabled = True
            
            # Se não estiver configurado ainda, apenas marca a flag
            else:
                instance._async_enabled = enabled
    
    @staticmethod
    def _configure_logging_system(max_size_mb: Optional[float] = 5.0) -> None:
        """
        Configura o sistema de logging padrão para uso com get_logger.
        
        Args:
            max_size_mb: Tamanho máximo do arquivo em megabytes antes da rotação
        """
        instance = Log._get_instance()
        
        if instance._logging_configured:
            return
        
        # Configurar logger raiz
        root_logger = logging.getLogger()
        root_logger.setLevel(instance._level)
        
        # Limpar handlers existentes
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Criar formatador
        formatter = logging.Formatter(instance._format_string)
        
        # Configurar saída para console
        if instance._use_console:
            instance._console_handler = logging.StreamHandler()
            instance._console_handler.setFormatter(formatter)
            root_logger.addHandler(instance._console_handler)
        
        # Configurar saída para arquivo se houver
        if instance._log_file_path:
            # Determinar o modo de abertura do arquivo
            file_mode = 'a'  # Sempre append no sistema logging
            
            # Criar handler com rotação
            instance._file_handler = RotatingFileHandler(
                instance._log_file_path,
                mode=file_mode,
                maxBytes=int(max_size_mb * 1024 * 1024) if max_size_mb else 0,
                backupCount=5,
                encoding='utf-8'
            )
            instance._file_handler.setFormatter(formatter)
            root_logger.addHandler(instance._file_handler)
        
        instance._logging_configured = True
        
        # Se async logging estiver habilitado, configurar também
        if instance._async_enabled:
            instance.configure_async_logging(True)
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Obtém um logger para o módulo especificado.
        Compatível com o padrão logging.getLogger(__name__).
        
        Args:
            name: Nome do módulo, normalmente o valor de __name__
            
        Returns:
            Um objeto Logger para o módulo especificado
        """
        instance = Log._get_instance()
        
        # Garantir que o sistema de logging está configurado
        if not instance._logging_configured:
            instance._configure_logging_system()
        
        # Retornar o logger para o módulo especificado
        return logging.getLogger(name)
    
    @staticmethod
    def set_module_level(module_name: str, level: LogLevel) -> None:
        """
        Define o nível de log para um módulo específico.
        
        Args:
            module_name: Nome do módulo para ajustar
            level: Nível de log para o módulo
        """
        instance = Log._get_instance()
        
        # Garantir que o sistema de logging está configurado
        if not instance._logging_configured:
            instance._configure_logging_system()
        
        # Configurar nível específico para o módulo
        logger = logging.getLogger(module_name)
        logger.setLevel(level)
    
    @staticmethod
    def _rotate_log_file() -> bool:
        """
        Rotaciona o arquivo de log atual criando um novo arquivo com timestamp.
        
        Returns:
            bool: True se o arquivo foi rotacionado com sucesso, False caso contrário
        """
        instance = Log._get_instance()
        
        if not instance._log_file_path:
            return False
            
        try:
            if instance._log_file is not None:
                instance._log_file.close()
                instance._log_file = None
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename, extension = os.path.splitext(instance._log_file_path)
            rotated_path = f"{filename}_{timestamp}{extension}"
            
            if os.path.exists(instance._log_file_path):
                os.rename(instance._log_file_path, rotated_path)
            
            instance._log_file = open(instance._log_file_path, 'w', encoding='utf-8')
            instance._current_file_size = 0
            
            rotation_message = f"Arquivo de log rotacionado. Arquivo anterior: {rotated_path}"
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            log_entry = instance._LOG_FORMAT.format(
                timestamp=now,
                name='system',
                level='INFO',
                thread_id=threading.get_ident(),
                filename=os.path.basename(__file__),
                lineno=0,
                message=rotation_message
            )
            
            instance._log_file.write(log_entry + '\n')
            instance._log_file.flush()
            instance._current_file_size += len(log_entry) + 1
            
            if instance._use_console:
                print(log_entry, file=sys.stdout, flush=True)
            
            return True
            
        except Exception as e:
            error_msg = f"Erro ao rotacionar arquivo de log: {str(e)}"
            sys.stderr.write(f"{error_msg}\n")
            
            try:
                instance._log_file = open(instance._log_file_path, 'a', encoding='utf-8')
            except Exception:
                instance._log_file = None
                
            return False
    
    @staticmethod
    def get_log_file_path() -> Optional[str]:
        """
        Retorna o caminho do arquivo de log atual.
        
        Returns:
            str ou None: Caminho do arquivo de log ou None se não configurado
        """
        instance = Log._get_instance()
        return instance._log_file_path
    
    @staticmethod
    def close_log_file() -> bool:
        """
        Fecha o arquivo de log atual, se estiver aberto.
        
        Returns:
            bool: True se o arquivo foi fechado com sucesso ou não havia arquivo 
                  aberto; False se ocorreu erro ao fechar o arquivo
        """
        instance = Log._get_instance()
        
        with instance._lock:
            success = True
            file_path = instance._log_file_path
            
            if instance._log_file is not None:
                try:
                    if instance._use_console:
                        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        console_msg = f"{now} - system - INFO - Arquivo de log {file_path} fechado com sucesso"
                        print(console_msg, file=sys.stdout, flush=True)
                    
                    instance._log_file.flush()
                    instance._log_file.close()

                except Exception as e:
                    success = False
                    error_msg = f"Erro ao fechar arquivo de log {file_path}: {str(e)}"
                    sys.stderr.write(f"{error_msg}\n")

                finally:
                    instance._log_file = None
                    instance._log_file_path = None
                    instance._current_file_size = 0
            
            return success
    
    @staticmethod
    def set_log_format(format_string: str) -> None:
        """
        Define um formato personalizado para as entradas de log.
        
        O formato deve conter variáveis como {timestamp}, {level}, {filename}, 
        {lineno}, {name}, {thread_id} e {message}.
        
        Args:
            format_string: String de formato para as entradas de log
        """
        instance = Log._get_instance()
        
        # Verificar se pelo menos as variáveis essenciais estão presentes
        required_vars = ['{timestamp}', '{level}', '{message}']
        for var in required_vars:
            if var not in format_string:
                raise ValueError(f"Formato de log inválido: variável {var} ausente")
        
        instance._LOG_FORMAT = format_string
        
        # Atualizar o formato para o sistema logging também
        # Convertendo o formato para o estilo do logging
        logging_format = format_string.replace('{timestamp}', '%(asctime)s')
        logging_format = logging_format.replace('{level}', '%(levelname)s')
        logging_format = logging_format.replace('{name}', '%(name)s')
        logging_format = logging_format.replace('{filename}', '%(filename)s')
        logging_format = logging_format.replace('{lineno}', '%(lineno)d')
        logging_format = logging_format.replace('{message}', '%(message)s')
        logging_format = logging_format.replace('{thread_id}', '%(thread)d')
        
        instance._format_string = logging_format
        
        # Atualizar os formatadores se já configurados
        if instance._logging_configured:
            formatter = logging.Formatter(instance._format_string)
            if instance._console_handler:
                instance._console_handler.setFormatter(formatter)

            if instance._file_handler:
                instance._file_handler.setFormatter(formatter)
    
    @staticmethod
    def get_log_format() -> str:
        """
        Retorna o formato atual das entradas de log.
        
        Returns:
            str: O formato atual de log
        """
        instance = Log._get_instance()
        return instance._LOG_FORMAT
    
    @staticmethod
    def set_max_file_size(max_size_mb: Optional[float]) -> None:
        """
        Define o tamanho máximo do arquivo de log antes da rotação.
        
        Args:
            max_size_mb: Tamanho máximo em megabytes. Use None para desativar a rotação.
        """
        instance = Log._get_instance()
        
        if max_size_mb is not None:
            instance._max_file_size_bytes = int(max_size_mb * 1024 * 1024)
        else:
            instance._max_file_size_bytes = None
            
        # Atualizar também o handler do rotating file se estiver configurado
        if instance._logging_configured and instance._file_handler:
            instance._file_handler.maxBytes = instance._max_file_size_bytes or 0
    
    # Funções para configurar contexto por thread
    @staticmethod
    def set_context(key: str, value: Any) -> None:
        """
        Define um valor de contexto para a thread atual.
        
        Args:
            key: Chave do contexto
            value: Valor associado à chave
        """
        if not hasattr(Log._context_data, 'data'):
            Log._context_data.data = {}
        Log._context_data.data[key] = value
    
    @staticmethod
    def clear_context() -> None:
        """
        Limpa todo o contexto da thread atual.
        """
        if hasattr(Log._context_data, 'data'):
            Log._context_data.data = {}
    
    @staticmethod
    def get_context() -> Dict[str, Any]:
        """
        Obtém o contexto da thread atual.
        
        Returns:
            Dict[str, Any]: Mapa de pares chave-valor do contexto atual
        """
        if not hasattr(Log._context_data, 'data'):
            Log._context_data.data = {}
        return Log._context_data.data.copy()
    
    @staticmethod
    def _log(level: LogLevel, message: Any, *args: Any, name: str = None, extra: Dict[str, Any] = None) -> None:
        """
        Método interno para processar e registrar mensagens de log.
        
        Args:
            level: Nível de severidade da mensagem
            message: Texto da mensagem ou template de formatação
            *args: Argumentos opcionais para formatação da mensagem
            name: Nome do logger (opcional, se não fornecido, será detectado automaticamente)
            extra: Informações adicionais para incluir no log
        """
        instance = Log._get_instance()
        
        if (level >= instance._level) and (instance._level != LogLevel.NONE):
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            level_name = level.name
            thread_id = threading.get_ident()
            
            # Obter informações sobre o chamador (arquivo e linha)
            frame = inspect.currentframe().f_back.f_back
            filename = os.path.basename(frame.f_code.co_filename)
            lineno = frame.f_lineno
            
            # Detectar o nome do módulo chamador se não fornecido
            if name is None:
                module = inspect.getmodule(frame)
                name = module.__name__ if module else 'unknown'
            
            if args:
                try:
                    message = message % args
                except Exception as e:
                    message = f"{message} (Erro de formatação: {str(e)}, Args: {args})"
            
            # Adicionar contexto da thread
            context = {}
            if hasattr(Log._context_data, 'data'):
                context.update(Log._context_data.data)
            
            # Adicionar informações extras
            if extra:
                context.update(extra)
            
            # Formatar mensagem
            log_entry = instance._LOG_FORMAT.format(
                timestamp=now,
                name=name,
                level=level_name,
                thread_id=thread_id,
                filename=filename,
                lineno=lineno,
                message=message
            )
            
            # Colorir log se necessário
            colored_log_entry = log_entry
            if instance._use_console and instance._use_colors:
                color_code = getattr(LogColors, level_name, LogColors.RESET)
                colored_log_entry = f"{color_code}{log_entry}{LogColors.RESET}"
            
            with instance._lock:
                if instance._use_console:
                    output = sys.stderr if level >= LogLevel.ERROR else sys.stdout
                    print(colored_log_entry, file=output, flush=True)
                
                if instance._log_file is not None:
                    try:
                        # Verificar se precisamos rotacionar o arquivo
                        entry_size = len(log_entry) + 1  # +1 para o \n
                        
                        if (
                            instance._max_file_size_bytes is not None and 
                            instance._current_file_size + entry_size > instance._max_file_size_bytes
                        ):
                            instance._rotate_log_file()
                        
                        # Escrever a entrada no arquivo de log
                        instance._log_file.write(log_entry + '\n')
                        instance._log_file.flush()
                        instance._current_file_size += entry_size

                    except Exception as e:
                        print(f"Erro ao escrever no arquivo de log: {str(e)}", file=sys.stderr, flush=True)
                        print(log_entry, file=sys.stderr, flush=True)
    
    @staticmethod
    def structured(level: LogLevel, **kwargs: Any) -> None:
        """
        Registra uma mensagem estruturada (formato JSON).
        
        Args:
            level: Nível de severidade da mensagem
            **kwargs: Pares chave-valor a serem incluídos no log
        """
        # Adicionar timestamp e nível de log se não fornecidos
        if 'timestamp' not in kwargs:
            kwargs['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        if 'level' not in kwargs:
            kwargs['level'] = level.name
        
        # Adicionar contexto
        context = Log.get_context()
        if context:
            kwargs['context'] = context
        
        # Detectar chamador
        frame = inspect.currentframe().f_back
        kwargs['filename'] = os.path.basename(frame.f_code.co_filename)
        kwargs['lineno'] = frame.f_lineno
        
        if 'name' not in kwargs:
            module = inspect.getmodule(frame)
            kwargs['name'] = module.__name__ if module else 'unknown'
            
        # Converter para JSON e logar
        json_msg = json.dumps(kwargs, default=str)
        Log._log(level, json_msg, name=kwargs.get('name'))
    
    @staticmethod
    def debug(message: Any, *args: Any, name: str = None, extra: Dict[str, Any] = None) -> None:
        """
        Registra uma mensagem com nível DEBUG.
        
        Args:
            message: Texto da mensagem ou template de formatação
            *args: Argumentos opcionais para formatação da mensagem
            name: Nome do logger (opcional)
            extra: Informações adicionais para incluir no log
        """
        Log._log(LogLevel.DEBUG, message, *args, name=name, extra=extra)
    
    @staticmethod
    def info(message: Any, *args: Any, name: str = None, extra: Dict[str, Any] = None) -> None:
        """
        Registra uma mensagem com nível INFO.
        
        Args:
            message: Texto da mensagem ou template de formatação
            *args: Argumentos opcionais para formatação da mensagem
            name: Nome do logger (opcional)
            extra: Informações adicionais para incluir no log
        """
        Log._log(LogLevel.INFO, message, *args, name=name, extra=extra)
    
    @staticmethod
    def warning(message: Any, *args: Any, name: str = None, extra: Dict[str, Any] = None) -> None:
        """
        Registra uma mensagem com nível WARNING.
        
        Args:
            message: Texto da mensagem ou template de formatação
            *args: Argumentos opcionais para formatação da mensagem
            name: Nome do logger (opcional)
            extra: Informações adicionais para incluir no log
        """
        Log._log(LogLevel.WARNING, message, *args, name=name, extra=extra)
    
    @staticmethod
    def error(message: Any, *args: Any, name: str = None, extra: Dict[str, Any] = None) -> None:
        """
        Registra uma mensagem com nível ERROR.
        
        Args:
            message: Texto da mensagem ou template de formatação
            *args: Argumentos opcionais para formatação da mensagem
            name: Nome do logger (opcional)
            extra: Informações adicionais para incluir no log
        """
        Log._log(LogLevel.ERROR, message, *args, name=name, extra=extra)
    
    @staticmethod
    def critical(message: Any, *args: Any, name: str = None, extra: Dict[str, Any] = None) -> None:
        """
        Registra uma mensagem com nível CRITICAL.
        
        Args:
            message: Texto da mensagem ou template de formatação
            *args: Argumentos opcionais para formatação da mensagem
            name: Nome do logger (opcional)
            extra: Informações adicionais para incluir no log
        """
        Log._log(LogLevel.CRITICAL, message, *args, name=name, extra=extra)
    
    @staticmethod
    def exception(message: str, *args: Any, exc_info: Optional[Exception] = None, name: str = None) -> None:
        """
        Registra uma exceção com detalhes completos do traceback.
        
        Args:
            message: Mensagem descritiva
            *args: Argumentos para formatação da mensagem
            exc_info: Exceção a ser registrada (default: exceção atual)
            name: Nome do logger
        """
        if exc_info is None:
            exc_info = sys.exc_info()[1]
        
        exc_message = f"{message} - Exceção: {exc_info.__class__.__name__}: {str(exc_info)}"
        Log._log(LogLevel.ERROR, exc_message, *args, name=name)
        Log._log(LogLevel.DEBUG, f"Traceback:\n{traceback.format_exc()}", name=name)
    
    @staticmethod
    def _cleanup_on_exit() -> None:
        """
        Método de cleanup registrado no atexit para garantir que os arquivos de log 
        sejam fechados corretamente quando o programa terminar.
        """
        instance = Log._get_instance()
        
        with instance._lock:
            # Desliga logging assíncrono se estiver ativo
            if instance._async_enabled and instance._listener:
                try:
                    instance._listener.stop()
                    instance._listener = None
                except Exception:
                    pass
                
            # Fecha arquivo de log
            if instance._log_file is not None:
                try:
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    log_entry = f"{now} - system - INFO - Encerrando o programa, fechando arquivo de log {instance._log_file_path}"
                    instance._log_file.write(log_entry + '\n')
                    instance._log_file.flush()
                    instance._log_file.close()
                except Exception as e:
                    sys.stderr.write(f"Erro ao fechar o arquivo de log durante encerramento: {str(e)}\n")

                finally:
                    instance._log_file = None
                    instance._log_file_path = None
                    instance._current_file_size = 0
            
            # Fechar também os handlers do logging padrão
            if instance._logging_configured:
                for handler in logging.getLogger().handlers:
                    if hasattr(handler, 'close'):
                        try:
                            handler.close()
                        except Exception:
                            pass


