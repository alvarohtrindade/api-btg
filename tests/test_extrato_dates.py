#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para testar disponibilidade de dados de extrato para múltiplas datas.

Autor: Claude (Assistente AI)
Data: 07/06/2025
Versão: 1.0.0
"""

import requests
import datetime
import json
import time
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any


ROOT_PATH = Path(__file__).resolve().parents(2)
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from dotenv import load_dotenv
import os


env_path = ROOT_PATH / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

AUTH_URL = os.getenv("AUTH_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCOPE = os.getenv("SCOPE_EXTRATO")
EXTRATO_URL = os.getenv("EXTRATO_URL", "https://funds.btgpactual.com/reports/Cash/FundAccountStatement")
TICKET_URL = os.getenv("TICKET_URL")

def get_token() -> str:
    """Obtém token de autenticação."""
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    
    resp = requests.post(AUTH_URL, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError("Token não obtido")
    return token

def test_date(date_str: str, token: str) -> Dict[str, Any]:
    """
    Testa uma data específica para ver se há dados disponíveis.
    Retorna informações sobre o resultado.
    """
    print(f"🧪 Testando {date_str}...")
    
    try:
        # Solicitar ticket
        headers = {
            "Accept": "application/json",
            "X-SecureConnect-Token": token,
            "Content-Type": "application/json-patch+json"
        }
        payload = {
            "contract": {
                "startDate": f"{date_str}T00:00:00.000Z",
                "endDate": f"{date_str}T23:59:59.000Z",
                "fundName": ""
            },
            "pageSize": 100,
            "webhookEndpoint": ""
        }
        
        resp = requests.post(EXTRATO_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        ticket = resp.json().get("ticket")
        
        if not ticket:
            return {
                "date": date_str,
                "status": "ERROR",
                "message": "Ticket não obtido",
                "has_data": False
            }
        
        # Aguardar um pouco
        time.sleep(3)
        
        # Tentar obter dados (máximo 3 tentativas rápidas)
        url = f"{TICKET_URL}?ticketId={ticket}&pageNumber=1"
        
        for attempt in range(1, 4):
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                
                if resp.status_code == 200:
                    data = resp.json()
                    result = data.get("result")
                    
                    if result == "Aguardando processamento" or result == "Processando":
                        if attempt < 3:
                            print(f"   ⏳ Tentativa {attempt}: Processando... aguardando 10s")
                            time.sleep(10)
                            continue
                        else:
                            return {
                                "date": date_str,
                                "status": "PROCESSING",
                                "message": "Ainda processando após 3 tentativas",
                                "has_data": None,
                                "attempts": attempt
                            }
                    
                    elif isinstance(result, list):
                        has_data = len(result) > 0
                        return {
                            "date": date_str,
                            "status": "SUCCESS",
                            "message": f"{'Dados encontrados' if has_data else 'Sem dados'} ({len(result)} registros)",
                            "has_data": has_data,
                            "record_count": len(result),
                            "total_pages": data.get("totalPages"),
                            "attempts": attempt
                        }
                    
                    else:
                        return {
                            "date": date_str,
                            "status": "UNKNOWN",
                            "message": f"Resposta inesperada: {result}",
                            "has_data": None,
                            "attempts": attempt
                        }
                        
                else:
                    if attempt < 3:
                        print(f"   ⚠️ Tentativa {attempt}: HTTP {resp.status_code}")
                        time.sleep(5)
                        continue
                    else:
                        return {
                            "date": date_str,
                            "status": "HTTP_ERROR",
                            "message": f"HTTP {resp.status_code}",
                            "has_data": False,
                            "attempts": attempt
                        }
                        
            except Exception as e:
                if attempt < 3:
                    print(f"   ❌ Tentativa {attempt}: {str(e)}")
                    time.sleep(5)
                    continue
                else:
                    return {
                        "date": date_str,
                        "status": "ERROR",
                        "message": f"Erro: {str(e)}",
                        "has_data": False,
                        "attempts": attempt
                    }
        
    except Exception as e:
        return {
            "date": date_str,
            "status": "ERROR",
            "message": f"Erro ao solicitar ticket: {str(e)}",
            "has_data": False
        }

def generate_test_dates(start_date: str, end_date: str, sample_count: int = 10) -> List[str]:
    """
    Gera uma amostra de datas para teste entre start_date e end_date.
    """
    start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    
    total_days = (end_dt - start_dt).days + 1
    
    if total_days <= sample_count:
        # Se há poucas datas, testar todas
        dates = []
        current = start_dt
        while current <= end_dt:
            dates.append(current.strftime('%Y-%m-%d'))
            current += datetime.timedelta(days=1)
        return dates
    else:
        # Amostragem espaçada
        step = total_days // sample_count
        dates = []
        for i in range(sample_count):
            test_date = start_dt + datetime.timedelta(days=i * step)
            if test_date <= end_dt:
                dates.append(test_date.strftime('%Y-%m-%d'))
        
        # Sempre incluir a última data
        if end_dt.strftime('%Y-%m-%d') not in dates:
            dates.append(end_dt.strftime('%Y-%m-%d'))
            
        return dates

def main():
    parser = argparse.ArgumentParser(description='Teste de disponibilidade de dados de extrato')
    parser.add_argument('--start-date', type=str, required=True, help='Data inicial (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True, help='Data final (YYYY-MM-DD)')
    parser.add_argument('--sample-count', type=int, default=10, help='Número de datas para amostrar')
    parser.add_argument('--output', type=str, help='Arquivo para salvar resultados (JSON)')
    
    args = parser.parse_args()
    
    print("🔍 === TESTE DE DISPONIBILIDADE DE DADOS DE EXTRATO ===")
    print(f"📅 Período: {args.start_date} até {args.end_date}")
    print(f"🎯 Amostragem: {args.sample_count} datas")
    print()
    
    try:
        # Validar datas
        datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
        datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
        
        # Obter token
        print("🔑 Obtendo token de autenticação...")
        token = get_token()
        print("✅ Token obtido com sucesso")
        print()
        
        # Gerar datas para teste
        test_dates = generate_test_dates(args.start_date, args.end_date, args.sample_count)
        print(f"📋 Datas selecionadas para teste: {len(test_dates)}")
        for date in test_dates:
            print(f"   • {date}")
        print()
        
        # Executar testes
        results = []
        for i, date in enumerate(test_dates, 1):
            print(f"[{i}/{len(test_dates)}] ", end="")
            result = test_date(date, token)
            results.append(result)
            
            # Status visual
            status = result["status"]
            has_data = result.get("has_data")
            
            if status == "SUCCESS" and has_data:
                print(f"   ✅ {result['message']}")
            elif status == "SUCCESS" and not has_data:
                print(f"   ⚪ {result['message']}")
            elif status == "PROCESSING":
                print(f"   ⏳ {result['message']}")
            else:
                print(f"   ❌ {result['message']}")
            
            # Pequena pausa entre testes para não sobrecarregar a API
            if i < len(test_dates):
                time.sleep(2)
        
        print()
        print("📊 === RESUMO DOS RESULTADOS ===")
        
        # Estatísticas
        total = len(results)
        with_data = sum(1 for r in results if r.get("has_data") is True)
        without_data = sum(1 for r in results if r.get("has_data") is False)
        processing = sum(1 for r in results if r["status"] == "PROCESSING")
        errors = sum(1 for r in results if r["status"] in ["ERROR", "HTTP_ERROR"])
        
        print(f"📈 Total de datas testadas: {total}")
        print(f"✅ Com dados: {with_data} ({with_data/total*100:.1f}%)")
        print(f"⚪ Sem dados: {without_data} ({without_data/total*100:.1f}%)")
        print(f"⏳ Ainda processando: {processing} ({processing/total*100:.1f}%)")
        print(f"❌ Erros: {errors} ({errors/total*100:.1f}%)")
        print()
        
        # Mostrar datas com dados
        dates_with_data = [r["date"] for r in results if r.get("has_data") is True]
        if dates_with_data:
            print("📅 Datas com dados confirmados:")
            for date in dates_with_data:
                result = next(r for r in results if r["date"] == date)
                count = result.get("record_count", "?")
                print(f"   • {date} ({count} registros)")
        else:
            print("⚠️ Nenhuma data com dados confirmados encontrada")
        
        print()
        
        # Salvar resultados se solicitado
        if args.output:
            output_file = Path(args.output)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "test_info": {
                        "start_date": args.start_date,
                        "end_date": args.end_date,
                        "sample_count": args.sample_count,
                        "total_tested": total,
                        "test_timestamp": datetime.datetime.now().isoformat()
                    },
                    "summary": {
                        "with_data": with_data,
                        "without_data": without_data,
                        "processing": processing,
                        "errors": errors
                    },
                    "results": results
                }, f, ensure_ascii=False, indent=2)
            print(f"💾 Resultados salvos em: {output_file}")
        
        # Recomendações
        print("💡 RECOMENDAÇÕES:")
        if with_data > 0:
            print(f"   • Use as datas com dados confirmados para testes")
            print(f"   • Datas mais recentes têm maior probabilidade de ter dados")
        
        if processing > 0:
            print(f"   • {processing} datas ainda estavam processando - teste novamente mais tarde")
        
        if without_data > with_data:
            print(f"   • A maioria das datas não possui dados - considere período mais recente")
        
        if errors > 0:
            print(f"   • {errors} datas tiveram erros - verifique conectividade e credenciais")
        
    except Exception as e:
        print(f"❌ Erro durante execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()