#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de Validação e Diagnóstico do ETL BTG

Autor: Claude (Assistente AI)
Data: 31/05/2025
Versão: 1.0
Descrição: Valida dependências, configurações e testa componentes do ETL BTG
"""

import os
import sys
import subprocess
import json
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime

# Ajustar ROOT_PATH
ROOT_PATH = Path(__file__).resolve().parents[2]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from utils.logging_utils import Log, LogLevel
from utils.mysql_connector_utils import MySQLConnector
from dotenv import load_dotenv

# Configurar logs
Log.set_level(LogLevel.INFO)
Log.set_console_output(True)
logger = Log.get_logger(__name__)

# Carregar .env
load_dotenv(ROOT_PATH / '.env')

class ETLValidator:
    """Classe para validação completa do ETL BTG"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.success_checks = []
        
    def log_success(self, message: str):
        """Registra um teste bem-sucedido"""
        self.success_checks.append(message)
        logger.info(f"✅ {message}")
        
    def log_warning(self, message: str):
        """Registra um aviso"""
        self.warnings.append(message)
        logger.warning(f"⚠️  {message}")
        
    def log_error(self, message: str):
        """Registra um erro"""
        self.errors.append(message)
        logger.error(f"❌ {message}")

    def check_python_version(self) -> bool:
        """Verifica se a versão do Python é adequada"""
        version = sys.version_info
        if version.major == 3 and version.minor >= 8:
            self.log_success(f"Versão Python OK: {version.major}.{version.minor}.{version.micro}")
            return True
        else:
            self.log_error(f"Versão Python inadequada: {version.major}.{version.minor}.{version.micro} (requer 3.8+)")
            return False

    def check_required_packages(self) -> bool:
        """Verifica se os pacotes Python necessários estão instalados"""
        required_packages = [
            'pandas', 'mysql-connector-python', 'python-dotenv', 
            'pydantic', 'jinja2', 'psutil', 'openpyxl'
        ]
        
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                self.log_success(f"Pacote {package} encontrado")
            except ImportError:
                missing_packages.append(package)
                self.log_error(f"Pacote {package} não encontrado")
        
        if missing_packages:
            self.log_error(f"Para instalar: pip install {' '.join(missing_packages)}")
            return False
        
        return True

    def check_environment_variables(self) -> bool:
        """Verifica se as variáveis de ambiente necessárias estão configuradas"""
        required_vars = [
            'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE',
            'MYSQL_TABLE', 'DB_RENTABILIDADE', 'SMTP_SERVER', 'SMTP_USERNAME',
            'SMTP_PASSWORD', 'RECEIVER_EMAIL'
        ]
        
        missing_vars = []
        
        for var in required_vars:
            value = os.getenv(var)
            if value:
                # Não mostrar senhas completas
                if 'PASSWORD' in var:
                    display_value = f"{value[:3]}***{value[-2:]}" if len(value) > 5 else "***"
                else:
                    display_value = value
                self.log_success(f"Variável {var} = {display_value}")
            else:
                missing_vars.append(var)
                self.log_error(f"Variável {var} não encontrada")
        
        if missing_vars:
            self.log_error("Verifique o arquivo .env")
            return False
        
        return True

    def check_mysql_connection(self) -> bool:
        """Testa a conexão com o banco MySQL"""
        try:
            connector = MySQLConnector.from_env()
            
            # Teste de conexão simples
            result = connector.query_single_value("SELECT 1 as test")
            if result == 1:
                self.log_success("Conexão MySQL estabelecida com sucesso")
                
                # Verificar se as tabelas existem
                tables_to_check = [
                    os.getenv('MYSQL_TABLE', 'Ft_CarteiraDiaria'),
                    os.getenv('DB_RENTABILIDADE', 'Ft_RentabilidadeDiaria')
                ]
                
                for table in tables_to_check:
                    try:
                        count = connector.query_single_value(f"SELECT COUNT(*) FROM {table}")
                        self.log_success(f"Tabela {table} existe com {count:,} registros")
                    except Exception as e:
                        if "doesn't exist" in str(e):
                            self.log_error(f"Tabela {table} não existe - execute o SQL de criação")
                        else:
                            self.log_error(f"Tabela {table} não encontrada ou inacessível: {e}")
                
                connector.close()
                return True
            else:
                self.log_error("Teste de conexão MySQL falhou")
                return False
                
        except Exception as e:
            self.log_error(f"Erro ao conectar ao MySQL: {e}")
            return False

    def check_script_files(self) -> bool:
        """Verifica se todos os scripts necessários existem"""
        base_dir = ROOT_PATH / "backend" / "api_btg"
        
        scripts_to_check = [
            ("Orquestrador Principal", base_dir / "orquestrador_btg.py"),
            ("Extração Portfolio", base_dir / "api_faas_portfolio.py"),
            ("Extração Rentabilidade", base_dir / "api_faas_rentabilidade.py"),
            ("Inserção Carteira", base_dir / "insert_db" / "insert_carteira.py"),
            ("Inserção Rentabilidade", base_dir / "insert_db" / "insert_rentabilidade.py"),
        ]
        
        all_found = True
        
        for name, path in scripts_to_check:
            if path.exists():
                self.log_success(f"Script {name} encontrado: {path.name}")
            else:
                self.log_error(f"Script {name} não encontrado: {path}")
                all_found = False
        
        return all_found

    def check_configuration_files(self) -> bool:
        """Verifica se os arquivos de configuração existem"""
        config_files = [
            ("Mapeamento de Colunas", ROOT_PATH / "configs" / "mappings" / "column_mapping.json"),
            ("Mapeamento de Fundos", ROOT_PATH / "configs" / "mappings" / "fund_mapping.json"),
            ("Mapeamento de Descrições", ROOT_PATH / "configs" / "mappings" / "descricao_mapping.json"),
            ("Mapeamento de Grupos", ROOT_PATH / "configs" / "mappings" / "grupo_mapping.json"),
            ("Mapeamento de Tipos", ROOT_PATH / "configs" / "mappings" / "fund_type_mapping.json"),
            ("Template de Email", ROOT_PATH / "configs" / "templates" / "btg_carteira_report.html"),
        ]
        
        all_found = True
        
        for name, path in config_files:
            if path.exists():
                # Verificar se JSONs são válidos
                if path.suffix == '.json':
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            json.load(f)
                        self.log_success(f"Arquivo {name} válido: {path.name}")
                    except json.JSONDecodeError as e:
                        self.log_error(f"Arquivo {name} tem JSON inválido: {e}")
                        all_found = False
                else:
                    self.log_success(f"Arquivo {name} encontrado: {path.name}")
            else:
                self.log_error(f"Arquivo {name} não encontrado: {path}")
                all_found = False
        
        return all_found

    def check_directories(self) -> bool:
        """Verifica se os diretórios necessários existem e são escribíveis"""
        directories_to_check = [
            ("Dados BTG", ROOT_PATH / "data" / "btg"),
            ("Logs", ROOT_PATH / "backend" / "api_btg" / "logs"),
            ("Configs", ROOT_PATH / "configs"),
            ("Utils", ROOT_PATH / "utils"),
        ]
        
        all_ok = True
        
        for name, path in directories_to_check:
            if path.exists():
                if os.access(path, os.W_OK):
                    self.log_success(f"Diretório {name} OK: {path}")
                else:
                    self.log_warning(f"Diretório {name} não é escribível: {path}")
            else:
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    self.log_success(f"Diretório {name} criado: {path}")
                except Exception as e:
                    self.log_error(f"Não foi possível criar diretório {name}: {e}")
                    all_ok = False
        
        return all_ok

    def test_script_help(self, script_path: Path, script_name: str) -> bool:
        """Testa se um script responde ao --help"""
        try:
            result = subprocess.run(
                [sys.executable, str(script_path), "--help"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                self.log_success(f"Script {script_name} responde ao --help")
                return True
            else:
                self.log_error(f"Script {script_name} falhou no teste --help: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.log_error(f"Script {script_name} demorou muito para responder")
            return False
        except Exception as e:
            self.log_error(f"Erro ao testar script {script_name}: {e}")
            return False

    def run_basic_tests(self) -> bool:
        """Executa testes básicos de funcionamento"""
        base_dir = ROOT_PATH / "backend" / "api_btg"
        
        tests = [
            (base_dir / "orquestrador_btg.py", "Orquestrador"),
            (base_dir / "api_faas_portfolio.py", "Extração Portfolio"),
            (base_dir / "api_faas_rentabilidade.py", "Extração Rentabilidade"),
            (base_dir / "insert_db" / "insert_carteira.py", "Inserção Carteira"),
            (base_dir / "insert_db" / "insert_rentabilidade.py", "Inserção Rentabilidade"),
        ]
        
        all_passed = True
        
        for script_path, script_name in tests:
            if script_path.exists():
                if not self.test_script_help(script_path, script_name):
                    all_passed = False
            else:
                self.log_error(f"Script {script_name} não encontrado para teste")
                all_passed = False
        
        return all_passed

    def generate_report(self) -> Dict[str, Any]:
        """Gera relatório final da validação"""
        total_checks = len(self.success_checks) + len(self.warnings) + len(self.errors)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_checks": total_checks,
                "successes": len(self.success_checks),
                "warnings": len(self.warnings),
                "errors": len(self.errors),
                "overall_status": "PASS" if len(self.errors) == 0 else "FAIL"
            },
            "details": {
                "successes": self.success_checks,
                "warnings": self.warnings,
                "errors": self.errors
            }
        }
        
        return report

    def run_full_validation(self) -> bool:
        """Executa validação completa"""
        logger.info("🔍 Iniciando validação completa do ETL BTG...")
        
        # Lista de verificações a executar
        checks = [
            ("Versão Python", self.check_python_version),
            ("Pacotes Python", self.check_required_packages),
            ("Variáveis de Ambiente", self.check_environment_variables),
            ("Conexão MySQL", self.check_mysql_connection),
            ("Scripts", self.check_script_files),
            ("Arquivos de Configuração", self.check_configuration_files),
            ("Diretórios", self.check_directories),
            ("Testes Básicos", self.run_basic_tests),
        ]
        
        for check_name, check_function in checks:
            logger.info(f"\n🔸 Executando: {check_name}")
            try:
                check_function()
            except Exception as e:
                self.log_error(f"Erro durante {check_name}: {e}")
        
        # Gerar relatório
        report = self.generate_report()
        
        # Mostrar resumo
        print(f"\n{'='*60}")
        print(f"📋 RELATÓRIO DE VALIDAÇÃO - ETL BTG")
        print(f"{'='*60}")
        print(f"✅ Sucessos: {report['summary']['successes']}")
        print(f"⚠️  Avisos: {report['summary']['warnings']}")
        print(f"❌ Erros: {report['summary']['errors']}")
        print(f"🎯 Status Geral: {report['summary']['overall_status']}")
        print(f"{'='*60}\n")
        
        if self.errors:
            print("❌ ERROS ENCONTRADOS:")
            for error in self.errors:
                print(f"   • {error}")
            print()
        
        if self.warnings:
            print("⚠️  AVISOS:")
            for warning in self.warnings:
                print(f"   • {warning}")
            print()
        
        return len(self.errors) == 0

def main():
    parser = argparse.ArgumentParser(description="Validação e Diagnóstico do ETL BTG")
    parser.add_argument('--json-output', type=str, help='Salvar relatório em arquivo JSON')
    parser.add_argument('--quick', action='store_true', help='Executar apenas verificações rápidas')
    args = parser.parse_args()
    
    validator = ETLValidator()
    
    if args.quick:
        logger.info("🏃 Executando validação rápida...")
        success = (
            validator.check_python_version() and
            validator.check_required_packages() and
            validator.check_environment_variables() and
            validator.check_script_files()
        )
    else:
        success = validator.run_full_validation()
    
    # Salvar relatório em JSON se solicitado
    if args.json_output:
        report = validator.generate_report()
        with open(args.json_output, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"📄 Relatório salvo em: {args.json_output}")
    
    # Código de saída
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()