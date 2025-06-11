"""
File: backoff_utils.py
Author: Cesar Godoy
Date: 2025-04-06
Version: 1.0
Description: Utilitários de resiliência com backoff exponencial e jitter para 
             melhorar a estabilidade de operações em ambientes distribuídos e instáveis.
             Implementa decorators para retry automático e circuit breaker, permitindo
             maior resiliência em sistemas distribuídos e evitando falhas em cascata.
"""

import time
import random
import functools
from typing import Any, Callable, Optional, Type, Tuple, Dict, Union
from dataclasses import dataclass
from threading import Lock

from utils.logging_utils import Log, LogLevel


# Exceções personalizadas para operações de resiliência
class ResilienceError(Exception):
    """Exceção base para erros relacionados a mecanismos de resiliência."""
    pass


class CircuitBreakerOpenError(ResilienceError):
    """Exceção lançada quando o circuit breaker está aberto e impede a chamada."""
    pass


class RetryExhaustedError(ResilienceError):
    """Exceção lançada quando todas as tentativas de retry foram esgotadas."""
    pass


def with_backoff_jitter(
    max_attempts: int = 3, 
    base_wait: float = 1.0, 
    jitter: float = 0.5,
    logger: Any = None,
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None
) -> Callable:
    """
    Decorator para retry de função com backoff exponencial e jitter.
    
    Args:
        max_attempts (int, opcional): 
            Número máximo de tentativas de execução da função. 
            Padrão é 3 tentativas.
        
        base_wait (float, opcional): 
            Tempo base de espera inicial em segundos entre as tentativas. 
            O tempo de espera aumenta exponencialmente a cada tentativa. 
            Padrão é 1.0 segundo.
        
        jitter (float, opcional): 
            Fator de variação aleatória para o tempo de espera. 
            Ajuda a prevenir sincronização de retries em sistemas distribuídos. 
            Valor entre 0 e 1. Padrão é 0.5 (50% de variação).
        
        logger (Any, opcional): 
            Objeto de logging para registrar informações sobre as tentativas. 
            Se não fornecido, usa o Log padrão do sistema.
        
        retryable_exceptions (tuple, opcional): 
            Tupla de tipos de exceções que devem acionar o retry. 
            Se não especificado, usa Exception como padrão.
    
    Returns:
        Callable: Função decorada com mecanismo de retry
    
    Example:
        @with_backoff_jitter(max_attempts=5, base_wait=2.0)
        def unstable_network_call():
            # Função que pode falhar intermitentemente
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Configura o logger
            log = logger or Log
            
            # Define exceções que serão retry
            exceptions = retryable_exceptions or (Exception,)
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                
                except exceptions as e:
                    # Se for a última tentativa, relança a exceção
                    if attempt == max_attempts:
                        log.error(
                            f'Falha na função {func.__name__} após {max_attempts} tentativas. '
                            f'Exceção final: {str(e)}', 
                            name='backoff_utils'
                        )
                        raise RetryExhaustedError(
                            f'Retry esgotado após {max_attempts} tentativas: {str(e)}'
                        ) from e
                    
                    # Calcula o tempo de espera com backoff exponencial e jitter
                    wait_time = base_wait * (2 ** (attempt - 1))
                    jitter_value = random.uniform(0, jitter * wait_time)
                    total_wait = wait_time + jitter_value
                    
                    log.warning(
                        f'Tentativa {attempt} de {max_attempts} falhou. '
                        f'Tempo de espera antes da próxima tentativa: {total_wait:.2f}s. '
                        f'Exceção: {str(e)}', 
                        name='backoff_utils'
                    )
                    
                    # Pausa antes da próxima tentativa
                    time.sleep(total_wait)
        
        return wrapper
    return decorator


@dataclass
class CircuitBreakerState:
    """
    Estado do circuit breaker.
    
    Attributes:
        failure_count: Contador de falhas consecutivas
        last_failure_time: Timestamp da última falha
        is_open: Indica se o circuit breaker está aberto (não permitindo chamadas)
        total_calls: Total de chamadas feitas
        successful_calls: Total de chamadas bem-sucedidas
        failed_calls: Total de chamadas com falha
    """
    failure_count: int = 0
    last_failure_time: float = 0
    is_open: bool = False
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0


_circuit_breakers: Dict[str, CircuitBreakerState] = {}
_circuit_breaker_lock = Lock()


def with_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    reset_timeout: float = 60.0,
    excluded_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    logger: Any = None
) -> Callable:
    """
    Decorator que implementa o padrão Circuit Breaker para evitar falhas em cascata.
    
    Args:
        name (str):
            Nome único para este circuit breaker. Usado para identificar o circuit breaker
            em logs e para rastrear seu estado.
            
        failure_threshold (int, opcional):
            Número de falhas consecutivas que disparam a abertura do circuit breaker.
            Padrão é 5 falhas.
            
        reset_timeout (float, opcional):
            Tempo em segundos que o circuit breaker permanece aberto antes de 
            permitir uma nova tentativa (estado half-open). Padrão é 60 segundos.
            
        excluded_exceptions (tuple, opcional):
            Tupla de tipos de exceções que não devem ser contabilizadas como falhas.
            Útil para exceções esperadas ou que não indicam problemas com o serviço.
            
        logger (Any, opcional):
            Objeto de logging para registrar informações sobre o circuit breaker.
            Se não fornecido, usa o Log padrão do sistema.
    
    Returns:
        Callable: Função decorada com o padrão circuit breaker
        
    Raises:
        CircuitBreakerOpenError: Quando o circuit breaker está aberto e impede a chamada
        
    Example:
        @with_circuit_breaker(name='database', failure_threshold=3, reset_timeout=30.0)
        def database_operation():
            # Operação que acessa banco de dados e pode falhar
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Configura o logger
            log = logger or Log
            
            # Inicializa o estado do circuit breaker se necessário
            with _circuit_breaker_lock:
                if name not in _circuit_breakers:
                    _circuit_breakers[name] = CircuitBreakerState()
                
                circuit_state = _circuit_breakers[name]
                
                # Verifica se o circuit breaker está aberto
                if circuit_state.is_open:
                    # Verifica se o timeout de reset já passou
                    elapsed = time.time() - circuit_state.last_failure_time
                    if elapsed >= reset_timeout:
                        # Coloca o circuit breaker em estado half-open para permitir uma tentativa
                        log.info(
                            f'Circuit breaker "{name}" em estado half-open após {elapsed:.1f}s',
                            name='backoff_utils'
                        )
                        circuit_state.is_open = False
                    else:
                        # Circuit breaker ainda está aberto, bloqueia a chamada
                        log.warning(
                            f'Circuit breaker "{name}" aberto. Bloqueando chamada. '
                            f'Tentará novamente em {reset_timeout - elapsed:.1f}s',
                            name='backoff_utils'
                        )
                        raise CircuitBreakerOpenError(
                            f'Circuit breaker "{name}" aberto. '
                            f'Tente novamente em {reset_timeout - elapsed:.1f}s'
                        )
                
                # Incrementa o total de chamadas
                circuit_state.total_calls += 1
            
            try:
                # Executa a função
                result = func(*args, **kwargs)
                
                # Registra sucesso
                with _circuit_breaker_lock:
                    circuit_state = _circuit_breakers[name]
                    circuit_state.failure_count = 0
                    circuit_state.successful_calls += 1
                    
                    if circuit_state.is_open:
                        # Se estava em half-open e teve sucesso, fecha o circuit breaker
                        circuit_state.is_open = False
                        log.info(
                            f'Circuit breaker "{name}" fechado após chamada bem-sucedida',
                            name='backoff_utils'
                        )
                
                return result
                
            except Exception as e:
                # Verifica se a exceção deve ser excluída da contagem de falhas
                if excluded_exceptions and isinstance(e, excluded_exceptions):
                    # Exceção excluída, não conta como falha
                    raise
                
                # Registra falha
                with _circuit_breaker_lock:
                    circuit_state = _circuit_breakers[name]
                    circuit_state.failure_count += 1
                    circuit_state.failed_calls += 1
                    circuit_state.last_failure_time = time.time()
                    
                    # Verifica se deve abrir o circuit breaker
                    if not circuit_state.is_open and circuit_state.failure_count >= failure_threshold:
                        circuit_state.is_open = True
                        log.error(
                            f'Circuit breaker "{name}" aberto após {circuit_state.failure_count} '
                            f'falhas consecutivas. Última exceção: {str(e)}',
                            name='backoff_utils'
                        )
                    else:
                        log.warning(
                            f'Falha registrada no circuit breaker "{name}" '
                            f'({circuit_state.failure_count}/{failure_threshold}). '
                            f'Exceção: {str(e)}',
                            name='backoff_utils'
                        )
                
                # Relança a exceção original
                raise
                
        return wrapper
    return decorator


def reset_circuit_breaker(name: str) -> bool:
    """
    Reseta um circuit breaker para o estado fechado.
    
    Args:
        name: Nome do circuit breaker
        
    Returns:
        True se o circuit breaker foi resetado, False se não existir
    """
    with _circuit_breaker_lock:
        if name not in _circuit_breakers:
            return False
            
        state = _circuit_breakers[name]
        state.is_open = False
        state.failure_count = 0
        Log.info(f'Circuit breaker "{name}" foi resetado manualmente', name='backoff_utils')
        return True


def get_circuit_breaker_stats(name: str) -> Optional[Dict[str, Any]]:
    """
    Retorna estatísticas sobre o estado atual de um circuit breaker.
    
    Args:
        name: Nome do circuit breaker
        
    Returns:
        Dicionário com estatísticas ou None se o circuit breaker não existir
    """
    with _circuit_breaker_lock:
        if name not in _circuit_breakers:
            return None
            
        state = _circuit_breakers[name]
        return {
            'name': name,
            'is_open': state.is_open,
            'failure_count': state.failure_count,
            'last_failure_time': state.last_failure_time,
            'total_calls': state.total_calls,
            'successful_calls': state.successful_calls,
            'failed_calls': state.failed_calls,
            'success_rate': (state.successful_calls / state.total_calls * 100) if state.total_calls > 0 else 0,
            'time_since_last_failure': time.time() - state.last_failure_time if state.last_failure_time > 0 else None
        }