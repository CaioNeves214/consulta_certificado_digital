from cryptography.hazmat.primitives.serialization import pkcs12
from con_database import con_banco_local, con_banco_remoto
from pathlib import Path


# Conexao com os bancos de dados
local_connection, local_cursor = con_banco_local()
remote_connection, remote_cursor = con_banco_remoto()

# Consulta para obter os dados das empresas
try: 
    local_cursor.execute("SELECT CGC, SENHACERTIFICADO, CAMINHOCERTIFICADO FROM empresas;")
    empresas = local_cursor.fetchall()
except Exception as e:
    print(f"Erro ao executar scriptSQL: {e}")

for cnpj, password, cert_path in empresas:
    if not cert_path:
        continue
    cert_path = Path(cert_path)

    try: 
        with open(cert_path, "rb") as cert_file:
            pfx_data = cert_file.read()

        cert = pkcs12.load_key_and_certificates(pfx_data, password.encode('utf-8'))
        validade_cert = cert[1].not_valid_after_utc.date()
    except Exception as e:
        print(f"Erro ao ler o certificado: {e}")
    
    # cursor_remote.execute("SELECT CGC FROM rotina_cliente WHERE CGC = %s", (cnpj,))
    # if (cursor_remote.fetchone() == None):
    #     cursor_remote.execute("INSERT INTO rotina_cliente (CGC, validade_certificado) VALUES (%s, %s)", (cnpj, validade_cert))

    # cursor_remote.execute("UPDATE rotina_cliente SET validade_certificado = %s WHERE CGC = %s", (validade_cert, cnpj))
    # connection_remote.commit()

local_cursor.close()
remote_cursor.close()
