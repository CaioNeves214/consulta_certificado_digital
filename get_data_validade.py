from cryptography.hazmat.primitives.serialization import pkcs12
from con_database import con_banco_local, con_banco_remoto
from pathlib import Path

def get_empresas(cursor):
    """Consulta para obter os dados das empresas"""
    try: 
        cursor.execute("SELECT CGC, SENHACERTIFICADO, CAMINHOCERTIFICADO FROM empresas;")
    except Exception as e:
        print(f"Erro ao executar scriptSQL: {e}")
        return []
    finally:
        return cursor.fetchall()

def get_validade_certificado(cert_path, password):
    """Obtem a validade do certificado"""
    try: 
        with open(cert_path, "rb") as cert_file:
            pfx_data = cert_file.read()

        cert = pkcs12.load_key_and_certificates(pfx_data, password.encode('utf-8'))
        return cert[1].not_valid_after_utc.date()
    except Exception as e:
        print(f"Erro ao ler o certificado: {e}")
        return None

def main():
    """Funcao principal para obter validade do certificado da empresa"""

    # Conexao com os bancos de dados
    local_connection, local_cursor = con_banco_local()
    remote_connection, remote_cursor = con_banco_remoto()

    empresas = get_empresas(local_cursor)
    for cnpj, password, cert_path in empresas:
        if not cert_path:
            continue
        cert_path = Path(cert_path)
        validade_cert = get_validade_certificado(cert_path, password)
        print(f"CNPJ: {cnpj}, Validade do Certificado: {validade_cert}")
    
    local_cursor.close()
    local_connection.close()
    remote_cursor.close()
    remote_connection.close()

if __name__ == "__main__":
    main()
