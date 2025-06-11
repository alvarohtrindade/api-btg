# Projeto BTG ETL - Grupo Catálise

## 1. Estrutura do Projeto

* Autor: Álvaro Trindade - Catálise Investimentos
* Date: 20/05/2025
* Version: 1.0

O projeto está organizado da seguinte forma:

```
/btg_etl_project/
├── apis/                        # Diretório com scripts de APIs
│   ├── api_faas_portfolio.py    # Extração de dados de carteira
│   └── api_faas_rentabilidade.py # Extração de dados de rentabilidade
├── guides/                      # Diretório de documentação
│   ├── README_Carteira.md       # Documentação de carteira
│   └── README_Rentabilidade.md  # Documentação de rentabilidade
├── insert_db/                   # Diretório com scripts de inserção
│   ├── insert_carteira.py       # Processamento e carga de dados de carteira
│   └── insert_rentabilidade.py  # Processamento e carga de dados de rentabilidade
├── logs/                        # Diretório para logs
├── mappings/                    # Diretório com arquivos de mapeamento
│   ├── map_nmfundos.json        # Mapeamento de nomes de fundos
│   └── map_description.json     # Mapeamento de descrições
├── .env                         # Configurações do ambiente (não versionado)
├── .gitignore                   # Configuração de exclusão de arquivos do Git
├── orquestrador_btg.py          # NOVO: Orquestrador de fluxo ETL
├── requirements.txt             # ATUALIZADO: Dependências do projeto
└── README.md             # Documento de revisão do projeto
```


## 2. Próximas melhorias

Para evolução futura do projeto, temos algumas melhorias mapeadas

1. **Novo endpoint - Extrato C/C**: Criar o script api_faas_extrato.py para obter arquivo de extrato conta corrente 

2. **Novo endpoint - Histórico de resgate**: Criar o script api_faas_resgate.py para obter arquivo histórico de resgate.

3. **Nova funcionalidade**: Ajustar orquestrador com novo comando CLI para extrair com filtro de datas consolidadas.

4. **Migração para Docker**: Containerizar a aplicação para facilitar a implantação e garantir consistência de ambiente

5. **Implantar utilitários de log, email, conector mysql e backoff**: Ajustar arquivos do projeto para utilizarem utilitários e boas práticas da área.

# Refatoração do Projeto BTG ETL

## Introdução

Este documento descreve a refatoração realizada no projeto BTG ETL para incorporar os novos utilitários desenvolvidos pela equipe. A refatoração mantém a funcionalidade original do sistema enquanto adiciona melhores práticas de código, tratamento de erros aprimorado, e novos recursos como notificações por e-mail e logging centralizado.

## Novos Arquivos

Adicionalmente, foram criados novos arquivos para dar suporte às novas funcionalidades:

1. **btg_email_utils.py** - Utilitário específico para envio de e-mails do projeto BTG
2. **email_templates/relatorio_carteira.html** - Template de e-mail para relatórios de carteira

## Principais Melhorias

### 1. Sistema de Logging Centralizado

- Substituído o sistema de logging básico pelo novo utilitário `logging_utils.py`
- Logs estruturados com níveis, timestamps e formato padronizado
- Configuração comum em todos os componentes
- Rotação de arquivos de log automática

### 2. Tratamento de Erros Aprimorado

- Adição de try/except em torno de blocos críticos
- Captura e reporte detalhado de exceções
- Estrutura robusta para logging de erros

### 3. Backoff e Retry para Operações Críticas

- Implementação de mecanismo de retry com backoff exponencial e jitter
- Decoradores `@with_backoff_jitter` aplicados em operações críticas:
  - Autenticação com a API
  - Requisições HTTP
  - Operações de banco de dados

### 4. Conector MySQL Melhorado

- Substituição da conexão direta por `MySQLConnector`
- Pool de conexões para melhor performance
- Métodos de alto nível para operações comuns
- Tratamento transacional adequado

### 5. Notificações por E-mail

- Integração com o sistema de notificações `notification_manager.py`
- Templates HTML para e-mails formatados
- Alertas de sucesso e falha
- Relatórios detalhados por e-mail

### 6. Tipagem com Type Hints

- Adição de anotações de tipo para melhor documentação do código
- Compatibilidade com ferramentas de análise estática
- Melhor compreensão das estruturas de dados

### 7. Documentação Aprimorada

- Docstrings completas em todas as funções
- Descrições claras dos parâmetros e valores de retorno
- Comentários explicativos em seções complexas

## Como Implementar as Alterações

### Preparação

1. Backup dos arquivos originais
2. Configurar as variáveis de ambiente necessárias (ver seção "Configuração")
3. Instalar as novas dependências (ver seção "Dependências")

### Substituição dos Arquivos

Para cada arquivo refatorado:

1. Renomear o arquivo original para adicionar um sufixo (ex: `_original`)
2. Copiar o novo arquivo para o diretório correspondente
3. Testar individualmente cada componente
4. Remover os arquivos originais após validação completa

### Novos Componentes

1. Criar o diretório `email_templates/` na raiz do projeto
2. Copiar os templates de e-mail para este diretório
3. Adicionar o arquivo `btg_email_utils.py` ao diretório adequado

## Configuração

Novas variáveis de ambiente para incluir no arquivo `.env`:

```
# Configurações de E-mail
BTG_EMAIL_RECIPIENTS=usuario1@empresa.com,usuario2@empresa.com
BTG_EMAIL_CC=gerente@empresa.com
BTG_EMAIL_SUBJECT_PREFIX=[BTG ETL]

# Configurações de Notificação
SMTP_SERVER=smtp.office365.com
SMTP_PORT=587
EMAIL_USERNAME=robot@empresa.com
EMAIL_PASSWORD=SENHA_SEGURA
```

## Dependências

Atualizar o arquivo `requirements.txt` para incluir:

```
pydantic==2.0.0
jinja2==3.1.2
```

## Uso dos Novos Recursos

### Envio de Notificações por E-mail

```python
from btg_email_utils import enviar_alerta_processamento

# Exemplo de uso
metricas = {
    "data_referencia": "2025-05-20",
    "extracao": {"total_fundos": 42},
    "processamento": {"total_registros": 1256}
}

# Enviar e-mail de alerta
enviar_alerta_processamento(
    tipo_processo="carteira",
    status="sucesso",
    metricas=metricas
)
```

### Uso do Backoff e Retry

```python
from utils.backoff_utils import with_backoff_jitter

# Exemplo de uso
@with_backoff_jitter(max_attempts=3, base_wait=2.0, jitter=0.5)
def funcao_com_retry():
    # Código que pode falhar e precisa de retry
    result = api_request()
    return result
```

### Uso do Conector MySQL

```python
from utils.mysql_connector_utils import MySQLConnector, MySQLConfig

# Exemplo de uso
config = MySQLConfig(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    pool_size=5
)

# Inicializar conector
mysql_connector = MySQLConnector(config)

# Executar query
results = mysql_connector.execute_query("SELECT * FROM tabela WHERE id = %s", (id_valor,))

# Inserir DataFrame
affected_rows = mysql_connector.execute_dataframe_insert(dataframe, "nome_tabela", batch_size=1000)

# Fechar conexão ao finalizar
mysql_connector.close()
```

## Considerações Futuras

1. **Monitoramento Proativo**: Implementar monitoramento proativo com alertas para falhas no processamento.
2. **Dashboard de Operações**: Criar um dashboard para visualização do status dos processos ETL.
3. **Testes Automatizados**: Adicionar testes automatizados para garantir a qualidade do código.
4. **CI/CD**: Implementar pipeline de integração e entrega contínua para automação de implantação.

## Conclusão

A refatoração do projeto BTG ETL incorpora novos utilitários que melhoram significativamente a robustez, manutenibilidade e recursos do sistema, mantendo a compatibilidade com as funcionalidades existentes. O código agora segue práticas mais modernas de desenvolvimento Python e está preparado para facilitar expansões futuras.
