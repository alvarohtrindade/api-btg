"""
File: email_utils.py
Author: Cesar Godoy
Date: 2025-04-08
Version: 1.1
Description: Utilitário para envio de e-mails via SMTP com validação de dados e tratamento de erros.
"""

import os
import re
import smtplib
import mimetypes
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from typing import Dict, List, Optional, Union
from string import Template
from dataclasses import dataclass

from utils.logging_utils import Log
from utils.backoff_utils import with_backoff_jitter, with_circuit_breaker

# Logger específico por módulo como recomendado no guia
logger = Log.get_logger(__name__)


@dataclass
class EmailConfig:
    """
    Configuração para envio de e-mails.
    """
    smtp_server: str
    port: int
    username: str
    password: str
    
    def __post_init__(self) -> None:
        """
        Valida os valores após inicialização.
        """
        if not self.smtp_server or not self.username or not self.password:
            raise ValueError('Parâmetros SMTP não podem ser vazios')
        if not (0 < self.port < 65536):
            raise ValueError(f"Porta SMTP inválida: {self.port}")


class EmailSender:
    """
    Classe para envio de emails com suporte a templates.
    """
    
    def __init__(
        self, 
        smtp_server: str, 
        port: int, 
        username: str, 
        password: str
    ) -> None:
        """
        Inicializa o sender com credenciais SMTP.
        """
        # Usar dataclass para validação como sugerido no guia
        self.config = EmailConfig(smtp_server, port, username, password)
        
        # Validação do formato de email
        if not self._validate_email_address(username):
            raise ValueError(f"Endereço de email inválido: {username}")
    
    @staticmethod
    def from_env() -> 'EmailSender':
        """
        Cria instância usando configurações do .env
        """
        from dotenv import load_dotenv
        load_dotenv()
        
        try:
            return EmailSender(
                smtp_server=os.getenv('SMTP_SERVER', ''),
                port=int(os.getenv('SMTP_PORT', '587')),
                username=os.getenv('SMTP_USERNAME', ''),
                password=os.getenv('SMTP_PASSWORD', '')
            )
        
        except ValueError as e:
            logger.error(f"Falha ao criar instância de EmailSender do .env: {str(e)}")
            raise
    
    def _validate_email_address(self, email: str) -> bool:
        """
        Valida formato de endereço de e-mail.
        """
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
        
    def render_template(
        self, 
        template_path: str, 
        context: Dict[str, str]
    ) -> Optional[str]:
        """
        Renderiza o template substituindo placeholders pelo contexto fornecido.
        
        Args:
            template_path: Caminho para o arquivo de template (HTML ou texto)
            context: Dicionário com variáveis para substituir no template
            
        Returns:
            String renderizada ou None se falhar
        """
        if not template_path:
            logger.warning('Template path vazio')

            return None

        try:
            path = Path(template_path)
            if not path.exists():
                logger.warning(f"Template não encontrado: {template_path}")

                return None

            with open(path, 'r', encoding='utf-8') as file:
                raw_template = file.read()
                template = Template(raw_template)

                return template.safe_substitute(context)

        except Exception as e:
            logger.warning(f"Erro ao renderizar template '{template_path}': {e}")
            return None
    
    def test_connection(self) -> bool:
        """
        Testa a conexão com o servidor SMTP.
        
        Returns:
            True se a conexão foi bem-sucedida, False caso contrário
        """
        try:
            with smtplib.SMTP(self.config.smtp_server, self.config.port, timeout=10) as server:
                server.starttls()
                server.login(self.config.username, self.config.password)
                logger.info('Conexão com servidor SMTP testada com sucesso')

                return True
            
        except Exception as e:
            logger.warning(f"Erro ao conectar ao servidor SMTP: {e}")
            return False
    
    @with_circuit_breaker(name='smtp_sender', failure_threshold=3, reset_timeout=30.0)
    def send_email(
        self, 
        to: Union[str, List[str]], 
        subject: str, 
        body: str, 
        is_html: bool = False,
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Envia um email usando as configurações SMTP definidas.
        
        Args:
            to: Destinatário(s) do email
            subject: Assunto do email
            body: Corpo do email (texto ou HTML)
            is_html: Se True, body é tratado como HTML
            cc: Endereços em cópia
            bcc: Endereços em cópia oculta
            attachments: Lista de caminhos para arquivos a anexar
            
        Returns:
            True se o email foi enviado com sucesso, False caso contrário
        """
        try:
            # Validar destinatários
            if isinstance(to, str):
                to = [to]
            
            for email in to:
                if not self._validate_email_address(email):
                    logger.warning(f"Destinatário com formato inválido: {email}")

                    return False
                    
            msg = EmailMessage()
            msg['Subject'] = subject
            msg['From'] = self.config.username
            msg['Date'] = formatdate(localtime=True)
            msg['To'] = ', '.join(to)
            
            if cc:
                if isinstance(cc, str): cc = [cc]
                msg['Cc'] = ', '.join(cc)
                
            if bcc:
                if isinstance(bcc, str): bcc = [bcc]
                msg['Bcc'] = ', '.join(bcc)
            
            # Definir o tipo de conteúdo e o corpo
            if is_html:
                msg.set_content(body, subtype='html')
            else:
                msg.set_content(body)
            
            # Adicionar anexos, se houver
            if attachments:
                self._attach_files(msg, attachments)
            
            # Enviar o email
            with smtplib.SMTP(self.config.smtp_server, self.config.port, timeout=10) as server:
                server.starttls()
                server.login(self.config.username, self.config.password)
                server.send_message(msg)
            
            logger.info(f"Email enviado com sucesso para {', '.join(to)}: {subject}")
            return True
            
        except Exception as e:
            logger.warning(f"Erro ao enviar email: {e}")
            return False
    
    def _attach_files(
        self, 
        msg: EmailMessage, 
        attachments: List[str]
    ) -> None:
        """
        Anexa arquivos à mensagem de email.
        
        Args:
            msg: Objeto EmailMessage para anexar os arquivos
            attachments: Lista de caminhos para os arquivos
        """
        for attachment_path in attachments:
            try:
                with open(attachment_path, 'rb') as f:
                    file_data = f.read()
                    file_name = os.path.basename(attachment_path)
                    
                mime_type, encoding = mimetypes.guess_type(attachment_path)
                if mime_type is None:
                    mime_type = 'application/octet-stream'
                    
                main_type, sub_type = mime_type.split('/', 1)
                msg.add_attachment(
                    file_data, 
                    maintype=main_type, 
                    subtype=sub_type, 
                    filename=file_name
                )
            except Exception as e:
                logger.warning(f"Erro ao anexar arquivo '{attachment_path}': {e}")
    
    @with_backoff_jitter(max_attempts=3, base_wait=2.0, jitter=0.3)
    def send_email_with_retry(
        self, 
        to: Union[str, List[str]], 
        subject: str, 
        body: str, 
        is_html: bool = False,
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Envia email com mecanismo de retry com backoff exponencial e jitter.
        
        Args:
            to: Destinatário(s) do email
            subject: Assunto do email
            body: Corpo do email (texto ou HTML)
            is_html: Se True, body é tratado como HTML
            cc: Endereços em cópia
            bcc: Endereços em cópia oculta
            attachments: Lista de caminhos para arquivos a anexar
            
        Returns:
            True se o email foi enviado com sucesso, False caso contrário
        """
        return self.send_email(to, subject, body, is_html, cc, bcc, attachments)
            
    def send_template_email(
        self, 
        to: Union[str, List[str]], 
        subject: str, 
        template_path: str,
        context: Dict[str, str],
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Renderiza um template e envia como email.
        
        Args:
            to: Destinatário(s) do email
            subject: Assunto do email
            template_path: Caminho para o arquivo de template
            context: Dicionário com variáveis para substituir no template
            cc: Endereços em cópia
            bcc: Endereços em cópia oculta
            attachments: Lista de caminhos para arquivos a anexar
            
        Returns:
            True se o email foi enviado com sucesso, False caso contrário
        """
        rendered = self.render_template(template_path, context)
        if not rendered:
            logger.warning(f"Falha ao renderizar template '{template_path}'")
            return False
            
        is_html = template_path.lower().endswith(('.html', '.htm'))
        return self.send_email(to, subject, rendered, is_html, cc, bcc, attachments)
        
    def send_template_email_with_retry(
        self, 
        to: Union[str, List[str]], 
        subject: str, 
        template_path: str,
        context: Dict[str, str],
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Renderiza um template e envia como email com mecanismo de retry.
        
        Args:
            to: Destinatário(s) do email
            subject: Assunto do email
            template_path: Caminho para o arquivo de template
            context: Dicionário com variáveis para substituir no template
            cc: Endereços em cópia
            bcc: Endereços em cópia oculta
            attachments: Lista de caminhos para arquivos a anexar
            
        Returns:
            True se o email foi enviado com sucesso, False caso contrário
        """
        rendered = self.render_template(template_path, context)
        if not rendered:
            logger.warning(f"Falha ao renderizar template '{template_path}'")
            return False
            
        is_html = template_path.lower().endswith(('.html', '.htm'))
        return self.send_email_with_retry(to, subject, rendered, is_html, cc, bcc, attachments)

    # TODO: Implementar versão assíncrona para melhor performance em alto volume
    # async def send_email_async(self, *args, **kwargs):
    #     """Versão assíncrona do send_email usando aiosmtplib"""
    #     pass
    
    # TODO: Integrar com AWS SQS para processamento assíncrono em alta escala
    # def queue_email_for_sending(self, *args, **kwargs):
    #     """Envia mensagem para fila SQS para processamento assíncrono"""
    #     pass