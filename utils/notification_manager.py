"""
Modulo: notification_manager.py
Autor: Cesar Godoy
Data: 2025-04-26
Versao: 1.1

Descricao:
    Gerencia o envio de notificacoes da aplicacao, com suporte inicial a e-mail via SMTP.
    Utiliza os modulos 'email_utils' (para envio de e-mails) e 'logging_utils' (para gerenciamento de logs).
    Permite configurar dinamicamente o nivel de log e a gravacao de logs em arquivo.
    Estruturado para expansao futura a outros canais como Microsoft Teams e Pipefy.
    Suporta envio para múltiplos canais simultaneamente através do tipo ALL.
"""

import os
import sys
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Union, List, Any, Tuple
from string import Template
from jinja2 import Template as JinjaTemplate

from pydantic import BaseModel, Field, field_validator, model_validator
from utils.logging_utils import Log, LogLevel
from utils.email_utils import EmailSender

# Constantes de configuração
DEFAULT_SMTP_SERVER = 'smtp.office365.com'
DEFAULT_SMTP_PORT = 587
DEFAULT_EMAIL = 'data_robots@example.com'
DEFAULT_LOG_DIR = 'logs'
MAX_LOG_SIZE_MB = 10.0

# Inicialização básica do logger para uso durante a configuração
logger = Log.get_logger(__name__)

class NotificationType(str, Enum):
    """
    Tipos de notificações suportados pelo gerenciador.
    
    Attributes:
        EMAIL: Canal de notificação por e-mail
        TEAMS: Canal de notificação via Microsoft Teams
        PIPEFY: Canal de notificação via Pipefy
        ALL: Envia para todos os canais disponíveis
    """
    EMAIL  = 'email'
    TEAMS  = 'teams'
    PIPEFY = 'pipefy'
    ALL    = 'all'  

class LogConfig(BaseModel):
    """
    Configuração do sistema de logging.
    
    Attributes:
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        to_file: Se True, ativa logging em arquivo
        log_dir: Diretório onde os logs serão salvos
        max_size_mb: Tamanho máximo do arquivo de log em MB
    """
    level: str = Field(default='INFO', description='Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    to_file: bool = Field(default=False, description='Se True, ativa logging em arquivo')
    log_dir: str = Field(default=DEFAULT_LOG_DIR, description='Diretório onde os logs serão salvos')
    max_size_mb: float = Field(default=MAX_LOG_SIZE_MB, description='Tamanho máximo do arquivo de log em MB')

    @field_validator('level')
    @classmethod
    def validate_level(cls, v):
        """Valida o nível de log e o converte para maiúsculas"""
        try:
            level_upper = v.upper()
            getattr(LogLevel, level_upper)
            return level_upper
        except (AttributeError, KeyError):
            raise ValueError(f"Nivel de log invalido: {v}")

class EmailCredentials(BaseModel):
    """
    Credenciais para o serviço de email.
    
    Attributes:
        server: Servidor SMTP
        port: Porta SMTP
        username: Nome de usuário para autenticação SMTP
        password: Senha para autenticação SMTP
    """
    server: str = Field(default=DEFAULT_SMTP_SERVER, description='Servidor SMTP')
    port: int = Field(default=DEFAULT_SMTP_PORT, description='Porta SMTP')
    username: str = Field(default=DEFAULT_EMAIL, description='Nome de usuário para autenticação SMTP')
    password: str = Field(default='', description='Senha para autenticação SMTP')

    @model_validator(mode='after')
    def check_password(self):
        """Valida se a senha foi fornecida"""
        if not self.password:
            logger.warning("Senha de email não configurada nas variáveis de ambiente")

        return self

class EmailNotification(BaseModel):
    """
    Configuração para envio de email.
    
    Attributes:
        to: Destinatário(s) do email
        subject: Assunto do email
        body: Corpo do email
        is_html: Se True, o conteúdo do email é HTML
        cc: Destinatário(s) em cópia
        bcc: Destinatário(s) em cópia oculta
        attachments: Lista de caminhos para arquivos a anexar
    """
    to: Union[str, List[str]]
    subject: str
    body: str
    is_html: bool = False
    cc: Optional[Union[str, List[str]]] = None
    bcc: Optional[Union[str, List[str]]] = None
    attachments: Optional[List[str]] = None

    @field_validator('to')
    @classmethod
    def validate_to(cls, v):
        """Valida se destinatários estão presentes"""
        if not v:
            raise ValueError("Destinatario do email nao especificado")
        
        return v

    @field_validator('body')
    @classmethod
    def validate_body(cls, v):
        """Valida se o corpo da mensagem está presente"""
        if not v:
            raise ValueError("Corpo do email nao especificado")
        
        return v

class TemplateNotification(BaseModel):
    """
    Configuração para envio de notificação baseada em template.
    
    Attributes:
        type: Tipo de notificação (EMAIL, TEAMS, PIPEFY, ALL)
        recipients: Destinatário(s) da notificação
        subject: Assunto/título da notificação
        template_path: Caminho para o arquivo de template
        context: Variáveis para preenchimento do template
        kwargs: Parâmetros adicionais específicos de cada canal
    """
    type: NotificationType
    recipients: Union[str, List[str]]
    subject: str
    template_path: str
    context: Dict[str, Any]
    kwargs: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('template_path')
    @classmethod
    def validate_template_path(cls, v):
        """Valida se o caminho do template existe"""
        if not v:
            raise ValueError("Path do template não especificado")
        
        return v

def render_template(template_path: str, context: Dict[str, Any]) -> Optional[str]:
    """
    Renderiza um template com Jinja2 usando o contexto fornecido.
    
    Args:
        template_path: Caminho para o arquivo de template
        context: Dicionário com variáveis
        
    Returns:
        Conteúdo renderizado
    """
    if not template_path:
        logger.warning("Template path vazio ou None")
        return None

    try:
        path = Path(template_path).resolve()
        if not path.exists():
            logger.warning(f"Template nao encontrado: {path}")
            return None

        with open(path, 'r', encoding='utf-8') as f:
            raw_template = f.read()
            template = JinjaTemplate(raw_template)
            return template.render(**context)

    except Exception as e:
        logger.error(f"Erro ao renderizar template '{template_path}': {e}")
        return None

class NotificationManager:
    """
    Gerencia o envio de notificações por diferentes canais.
    """
    
    def __init__(self, log_config: Optional[Union[LogConfig, Dict]] = None):
        """
        Inicializa o gerenciador de notificações.
        
        Args:
            log_config: Configuração de logging (objeto LogConfig ou dicionário)
        """
        # Converte configuração dict para objeto Pydantic se necessário
        if log_config is None:
            self.log_config = LogConfig()

        elif isinstance(log_config, dict):
            self.log_config = LogConfig(**log_config)

        else:
            self.log_config = log_config
            
        # Configuração de logging precisa vir primeiro
        self._configure_logging()
        
        # Configuração de email
        self.email_credentials = self._load_email_credentials()
        self.email_sender = None
        
        if self.email_credentials:
            self._setup_email_sender()

    def _configure_logging(self) -> None:
        """
        Configura o sistema de logging baseado nas configurações fornecidas.
        """
        global logger
        
        try:
            parsed_level = getattr(LogLevel, self.log_config.level, LogLevel.INFO)
            Log.set_level(parsed_level)
            Log.set_console_output(True)

            if self.log_config.to_file:
                module_name = Path(__file__).stem
                logs_dir = Path(self.log_config.log_dir)
                logs_dir.mkdir(exist_ok=True)
                log_file_path = logs_dir / f"{module_name}.log"
                if not Log.set_log_file(str(log_file_path), append=True, max_size_mb=self.log_config.max_size_mb):
                    logger.warning("Falha ao configurar log em arquivo. Mantendo apenas console.")
                else:
                    logger.info(f"Logging em arquivo ativado: {log_file_path}")
            
            logger.info(f"Nivel de log configurado: {parsed_level.name}")
            
            # Reaplicar o logger para ter as configurações atualizadas
            logger = Log.get_logger(__name__)

        except Exception as e:
            sys.stderr.write(f"Falha na configuracao de logging: {e}\n")
            Log.set_level(LogLevel.INFO)
            Log.set_console_output(True)
            logger = Log.get_logger(__name__)
            logger.info("Fallback aplicado para console e nivel INFO.")

    def _load_email_credentials(self) -> Optional[EmailCredentials]:
        """
        Carrega credenciais de email das variáveis de ambiente.
        
        Returns:
            EmailCredentials ou None em caso de erro
        """
        try:
            credentials = EmailCredentials(
                server=os.environ.get('SMTP_SERVER', DEFAULT_SMTP_SERVER),
                port=int(os.environ.get('SMTP_PORT', str(DEFAULT_SMTP_PORT))),
                username=os.environ.get('SMTP_USERNAME', DEFAULT_EMAIL),   
                password=os.environ.get('SMTP_PASSWORD', '')              
            )
            logger.info("Credenciais de email carregadas com sucesso")

            return credentials
        
        except Exception as e:
            logger.error(f"Erro ao carregar credenciais de email: {e}")
            return None

    def _setup_email_sender(self) -> None:
        """
        Configura o objeto EmailSender com as credenciais carregadas.
        """
        try:
            if self.email_credentials:
                self.email_sender = EmailSender(
                    smtp_server=self.email_credentials.server,
                    port=self.email_credentials.port,
                    username=self.email_credentials.username,
                    password=self.email_credentials.password
                )
                logger.info("EmailSender inicializado com sucesso")

            else:
                logger.warning("Não foi possível configurar EmailSender: credenciais ausentes")
                
        except Exception as e:
            logger.error(f"Erro ao configurar EmailSender: {e}")
            self.email_sender = None

    def is_ready(self) -> bool:
        """
        Verifica se o gerenciador está pronto para enviar notificações.
        
        Returns:
            True se estiver pronto para enviar notificações, False caso contrário
        """
        if self.email_sender is None:
            logger.warning("NotificationManager não está pronto: EmailSender não está configurado")
            return False
        return True

    def check_template_exists(self, template_path: str) -> bool:
        """
        Verifica se um template existe no caminho especificado.
        
        Args:
            template_path: Caminho para o arquivo de template
            
        Returns:
            True se o template existir, False caso contrário
        """
        if not template_path:
            return False
            
        try:
            path = Path(template_path).resolve()
            return path.exists() and path.is_file()
        
        except Exception as e:
            logger.error(f"Erro ao verificar existência do template: {e}")
            return False

    def get_available_channels(self) -> List[NotificationType]:
        """
        Retorna a lista de canais de notificação atualmente disponíveis.
        
        Returns:
            Lista de tipos de notificação disponíveis (excluindo ALL)
        """
        channels = []
        
        # EMAIL está disponível se o EmailSender estiver configurado
        if self.email_sender is not None:
            channels.append(NotificationType.EMAIL)
        
        # TEAMS e PIPEFY ainda não estão implementados, então não são incluídos
        # Quando forem implementados, adicionar verificações aqui
        
        return channels

    def send_notification(
        self, 
        type: NotificationType,
        recipients: Union[str, List[str]],
        subject: str,
        content: str,
        **kwargs: Any
    ) -> bool:
        """
        Método unificado para envio de notificações de qualquer tipo.
        
        Args:
            type: Tipo de notificação (EMAIL, TEAMS, PIPEFY, ALL)
            recipients: Destinatário(s) da notificação
            subject: Assunto/título da notificação
            content: Conteúdo principal da notificação
            **kwargs: Parâmetros adicionais específicos de cada canal
            
        Returns:
            True se pelo menos um canal foi enviado com sucesso, False caso contrário
        """
        # Se for ALL, enviar para todos os canais disponíveis
        if type == NotificationType.ALL:
            return self._send_to_all_channels(recipients, subject, content, **kwargs)
            
        # Verificar estado de prontidão para o canal específico
        if type == NotificationType.EMAIL and not self.is_ready():
            logger.error("NotificationManager não está pronto para enviar e-mails")
            return False
            
        # Log da operação com detalhes
        recipient_count = 1 if isinstance(recipients, str) else len(recipients)
        content_size = len(content) if content else 0
        logger.info(f"Enviando {type.value} para {recipient_count} destinatário(s). Tamanho do conteúdo: {content_size} caracteres")

        try:
            if type == NotificationType.EMAIL:
                email_data = EmailNotification(
                    to=recipients,
                    subject=subject,
                    body=content,
                    **kwargs
                )
                return self.send_email(email_data)
            
            elif type == NotificationType.TEAMS:
                return self.send_teams_message(recipients, subject, content, **kwargs)
            
            elif type == NotificationType.PIPEFY:
                return self.send_pipefy_card(recipients, subject, content, **kwargs)
            
            else:
                logger.error(f"Tipo de notificacao nao suportado: {type}")
                return False
            
        except ValueError as e:
            logger.error(f"Erro de validacao: {e}")
            return False

    def _send_to_all_channels(
        self,
        recipients: Union[str, List[str]],
        subject: str,
        content: str,
        **kwargs: Any
    ) -> bool:
        """
        Envia uma notificação para todos os canais disponíveis.
        
        Args:
            recipients: Destinatário(s) da notificação
            subject: Assunto/título da notificação
            content: Conteúdo principal da notificação
            **kwargs: Parâmetros adicionais específicos de cada canal
            
        Returns:
            True se pelo menos um canal foi enviado com sucesso, False caso contrário
        """
        logger.info("Enviando notificação para todos os canais disponíveis")
        
        # Obter canais disponíveis
        available_channels = self.get_available_channels()
        
        if not available_channels:
            logger.warning("Nenhum canal de notificação disponível")
            return False
        
        # Contadores de sucesso/falha
        success_count = 0
        failure_count = 0
        
        # Enviar para cada canal disponível
        for channel in available_channels:
            try:
                logger.info(f"Tentando enviar via canal: {channel.value}")
                result = self.send_notification(
                    type=channel,
                    recipients=recipients,
                    subject=subject,
                    content=content,
                    **kwargs
                )
                
                if result:
                    logger.info(f"Envio via {channel.value} bem-sucedido")
                    success_count += 1
                else:
                    logger.warning(f"Falha no envio via {channel.value}")
                    failure_count += 1
                    
            except Exception as e:
                logger.error(f"Erro ao enviar via {channel.value}: {e}")
                failure_count += 1
        
        # Registrar resumo do envio
        total = success_count + failure_count
        logger.info(f"Resumo do envio multi-canal: {success_count}/{total} canais com sucesso")
        
        # Retornar True se pelo menos um canal teve sucesso
        return success_count > 0

    def send_email(self, email_data: Union[EmailNotification, Dict]) -> bool:
        """
        Envia email via SMTP.
        
        Args:
            email_data: Objeto EmailNotification ou dicionário com dados do email
            
        Returns:
            True se enviado com sucesso, False caso contrário
        """
        try:
            # Converte dicionário para EmailNotification se necessário
            if isinstance(email_data, dict):
                email_data = EmailNotification(**email_data)
                
            if not self.email_sender:
                logger.error("EmailSender nao inicializado. Verifique as credenciais")
                return False

            # Log detalhado da operação
            to_count  = 1 if isinstance(email_data.to, str) else len(email_data.to)
            cc_count  = 0 if not email_data.cc  else (1 if isinstance(email_data.cc, str)  else len(email_data.cc))
            bcc_count = 0 if not email_data.bcc else (1 if isinstance(email_data.bcc, str) else len(email_data.bcc))
            attachments_count = 0 if not email_data.attachments else len(email_data.attachments)
            
            logger.info(
                f"Enviando email para {to_count} destinatário(s) principal(is), "
                f"{cc_count} em CC, {bcc_count} em BCC, "
                f"com {attachments_count} anexo(s)"
            )

            result = self.email_sender.send_email(
                to=email_data.to,
                subject=email_data.subject,
                body=email_data.body,
                is_html=email_data.is_html,
                cc=email_data.cc,
                bcc=email_data.bcc,
                attachments=email_data.attachments
            )
            
            if result:
                logger.info("Email enviado com sucesso")

            else:
                logger.error("Falha ao enviar email")
                
            return result
            
        except Exception as e:
            logger.error(f"Erro ao enviar email: {e}")
            return False

    def send_teams_message(
        self, 
        recipients: Union[str, List[str]], 
        title: str, 
        message: str, 
        **kwargs
    ) -> bool:
        """
        Envia mensagem para Microsoft Teams.
        
        Args:
            recipients: Destinatário(s) ou canais
            title: Título da mensagem
            message: Conteúdo da mensagem
            **kwargs: Parâmetros adicionais para a mensagem do Teams
            
        Returns:
            True se enviado com sucesso, False caso contrário
        """
        logger.warning("Envio para Teams ainda nao implementado")
        return False

    def send_pipefy_card(
        self, 
        board: Union[str, List[str]], 
        title: str, 
        description: str, 
        **kwargs
    ) -> bool:
        """
        Cria um card no Pipefy.
        
        Args:
            board: ID ou nome do board/pipe
            title: Título do card
            description: Descrição do card
            **kwargs: Campos adicionais do card
            
        Returns:
            True se criado com sucesso, False caso contrário
        """
        logger.warning("Envio para Pipefy ainda nao implementado")
        return False

    def send_with_template(
        self, 
        template_config: Union[TemplateNotification, Dict]
    ) -> Tuple[bool, Optional[str]]:
        """
        Envia notificação usando um template.
        
        Args:
            template_config: Objeto TemplateNotification ou dicionário com configuração do template
            
        Returns:
            Tuple com status de envio e mensagem em caso de erro
        """
        try:
            # Converte dicionário para TemplateNotification se necessário
            if isinstance(template_config, dict):
                template_config = TemplateNotification(**template_config)
            
            # Verifica se o template existe antes de tentar renderizá-lo
            if not self.check_template_exists(template_config.template_path):
                error_msg = f"Template não encontrado: {template_config.template_path}"
                logger.error(error_msg)
                return False, error_msg
                
            logger.info(f"Renderizando template: {template_config.template_path}")
            content = render_template(template_config.template_path, template_config.context)
            if not content:
                logger.error(f"Falha ao renderizar template: {template_config.template_path}")
                return False, "Falha ao renderizar template"

            # Log das variáveis de contexto (sem valores sensíveis)
            context_keys = list(template_config.context.keys())
            logger.info(f"Template renderizado com {len(context_keys)} variáveis de contexto: {', '.join(context_keys)}")

            success = self.send_notification(
                type=template_config.type,
                recipients=template_config.recipients,
                subject=template_config.subject,
                content=content,
                **template_config.kwargs
            )

            if success:
                logger.info(f"Notificacao enviada com sucesso usando template: {template_config.template_path}")
            else:
                logger.error(f"Falha ao enviar notificacao usando template: {template_config.template_path}")

            return success, None

        except ValueError as e:
            error_msg = str(e)
            logger.error(f"Erro de validacao no envio com template: {error_msg}")
            return False, error_msg