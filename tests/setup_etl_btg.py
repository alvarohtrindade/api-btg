#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de Inicialização e Setup do ETL BTG

Autor: Claude (Assistente AI)
Data: 31/05/2025
Versão: 1.0
Descrição: Configura automaticamente o ambiente para o ETL BTG
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
from typing import List

# Ajustar ROOT_PATH
ROOT_PATH = Path(__file__).resolve().parents[2]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

class ETLSetup:
    """Classe para configuração automatizada do ETL BTG"""
    
    def __init__(self):
        self.root_path = ROOT_PATH
        self.env_file = self.root_path / '.env'
        
    def install_packages(self) -> bool:
        """Instala os pacotes Python necessários"""
        packages = ['mysql-connector-python', 'python-dotenv']
        
        print(f"🔧 Instalando pacotes: {', '.join(packages)}")
        
        try:
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install'
            ] + packages)
            print("✅ Pacotes instalados com sucesso!")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Erro ao instalar pacotes: {e}")
            return False
    
    def update_env_file(self) -> bool:
        """Atualiza o arquivo .env com as variáveis necessárias"""
        print("🔧 Verificando arquivo .env...")
        
        # Verificar se DB_RENTABILIDADE existe
        env_content = ""
        if self.env_file.exists():
            with open(self.env_file, 'r', encoding='utf-8') as f:
                env_content = f.read()
        
        # Adicionar DB_RENTABILIDADE se não existir
        if 'DB_RENTABILIDADE' not in env_content:
            print("📝 Adicionando variável DB_RENTABILIDADE ao .env")
            
            # Adicionar ao final do arquivo
            with open(self.env_file, 'a', encoding='utf-8') as f:
                f.write('\n# Tabela de rentabilidade\n')
                f.write('DB_RENTABILIDADE=Ft_RentabilidadeDiaria\n')
            
            print("✅ Variável DB_RENTABILIDADE adicionada!")
        else:
            print("✅ Variável DB_RENTABILIDADE já existe")
        
        return True
    
    def create_rentabilidade_table_sql(self) -> str:
        """Retorna o SQL para criar a tabela de rentabilidade"""
        return """
-- Criação da tabela Ft_RentabilidadeDiaria
CREATE TABLE IF NOT EXISTS `Ft_RentabilidadeDiaria` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `NmFundo` varchar(255) DEFAULT NULL,
  `CdConta` varchar(100) DEFAULT NULL,
  `CnpjFundo` varchar(20) DEFAULT NULL,
  `DtPosicao` date DEFAULT NULL,
  `VlrCotacao` decimal(18,8) DEFAULT NULL,
  `VlrCotacaoBruta` decimal(18,8) DEFAULT NULL,
  `VlrPatrimonio` decimal(18,2) DEFAULT NULL,
  `QtdCota` decimal(18,8) DEFAULT NULL,
  `VlrAplicacao` decimal(18,2) DEFAULT NULL,
  `VlrResgate` decimal(18,2) DEFAULT NULL,
  `RentDia` decimal(10,6) DEFAULT NULL,
  `RentMes` decimal(10,6) DEFAULT NULL,
  `RentAno` decimal(10,6) DEFAULT NULL,
  `RentDiaVsCDI` decimal(10,6) DEFAULT NULL,
  `RentMesVsCDI` decimal(10,6) DEFAULT NULL,
  `RentAnoVsCDI` decimal(10,6) DEFAULT NULL,
  `TpClasse` varchar(100) DEFAULT NULL,
  `DtInclusao` timestamp DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_rentabilidade_fundo_data` (`NmFundo`,`DtPosicao`),
  KEY `idx_rentabilidade_data` (`DtPosicao`),
  KEY `idx_rentabilidade_cnpj` (`CnpjFundo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
    
    def create_database_table(self) -> bool:
        """Tenta criar a tabela de rentabilidade no banco"""
        print("🔧 Verificando tabela de rentabilidade...")
        
        try:
            # Importar após carregar .env
            from dotenv import load_dotenv
            load_dotenv(self.env_file)
            
            from utils.mysql_connector_utils import MySQLConnector
            
            connector = MySQLConnector.from_env()
            
            # Verificar se a tabela já existe
            try:
                count = connector.query_single_value("SELECT COUNT(*) FROM Ft_RentabilidadeDiaria")
                print(f"✅ Tabela Ft_RentabilidadeDiaria já existe com {count:,} registros")
                connector.close()
                return True
            except:
                # Tabela não existe, criar
                print("📝 Criando tabela Ft_RentabilidadeDiaria...")
                
                sql = self.create_rentabilidade_table_sql()
                connector.execute_update(sql)
                
                print("✅ Tabela Ft_RentabilidadeDiaria criada com sucesso!")
                connector.close()
                return True
                
        except Exception as e:
            print(f"❌ Erro ao criar tabela: {e}")
            print("📝 Execute manualmente o SQL:")
            print(self.create_rentabilidade_table_sql())
            return False
    
    def create_directories(self) -> bool:
        """Cria os diretórios necessários"""
        directories = [
            self.root_path / "data" / "btg" / "raw",
            self.root_path / "data" / "btg" / "extracted",
            self.root_path / "data" / "btg" / "raw_rent",
            self.root_path / "backend" / "api_btg" / "logs",
            self.root_path / "configs" / "mappings",
            self.root_path / "configs" / "templates",
        ]
        
        print("🔧 Criando diretórios necessários...")
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                print(f"✅ Diretório criado: {directory.relative_to(self.root_path)}")
            except Exception as e:
                print(f"❌ Erro ao criar {directory}: {e}")
                return False
        
        return True
    
    def run_validation(self) -> bool:
        """Executa o script de validação"""
        print("🔍 Executando validação final...")
        
        try:
            validate_script = self.root_path / "backend" / "api_btg" / "validate_etl_btg.py"
            
            if not validate_script.exists():
                print(f"❌ Script de validação não encontrado: {validate_script}")
                return False
            
            result = subprocess.run([
                sys.executable, str(validate_script), "--quick"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Validação executada com sucesso!")
                return True
            else:
                print(f"⚠️  Validação encontrou problemas:")
                print(result.stdout)
                return False
                
        except Exception as e:
            print(f"❌ Erro ao executar validação: {e}")
            return False
    
    def setup_complete(self) -> bool:
        """Executa o setup completo"""
        print("🚀 Iniciando setup do ETL BTG...")
        print("="*60)
        
        steps = [
            ("Instalação de Pacotes", self.install_packages),
            ("Atualização do .env", self.update_env_file),
            ("Criação de Diretórios", self.create_directories),
            ("Criação da Tabela de Rentabilidade", self.create_database_table),
            ("Validação Final", self.run_validation),
        ]
        
        success_count = 0
        
        for step_name, step_function in steps:
            print(f"\n🔸 {step_name}")
            try:
                if step_function():
                    success_count += 1
                else:
                    print(f"❌ Falha na etapa: {step_name}")
            except Exception as e:
                print(f"❌ Erro na etapa {step_name}: {e}")
        
        print("\n" + "="*60)
        print(f"📊 RESUMO DO SETUP")
        print("="*60)
        print(f"✅ Etapas concluídas: {success_count}/{len(steps)}")
        
        if success_count == len(steps):
            print("🎉 Setup concluído com SUCESSO!")
            print("\n🚀 Próximos passos:")
            print("   1. Configure as credenciais da API BTG no .env (CLIENT_ID, CLIENT_SECRET)")
            print("   2. Execute: python orquestrador_btg.py --date 2025-05-28")
            return True
        else:
            print("⚠️  Setup concluído com problemas. Verifique os erros acima.")
            return False

def main():
    parser = argparse.ArgumentParser(description="Setup automatizado do ETL BTG")
    parser.add_argument('--packages-only', action='store_true', help='Instalar apenas os pacotes')
    parser.add_argument('--env-only', action='store_true', help='Atualizar apenas o .env')
    parser.add_argument('--table-only', action='store_true', help='Criar apenas a tabela')
    args = parser.parse_args()
    
    setup = ETLSetup()
    
    if args.packages_only:
        success = setup.install_packages()
    elif args.env_only:
        success = setup.update_env_file()
    elif args.table_only:
        success = setup.create_database_table()
    else:
        success = setup.setup_complete()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()