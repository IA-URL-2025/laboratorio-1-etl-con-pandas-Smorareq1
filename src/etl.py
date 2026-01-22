import pandas as pd

def run_etl():
    """
    Implementa el proceso ETL.
    No cambies el nombre de esta función.
    """
    df = pd.read_csv('data/citas_clinica.csv')
    
    df['paciente'] = df['paciente'].str.title()
    df['especialidad'] = df['especialidad'].str.upper()
    
    df['fecha_cita'] = pd.to_datetime(df['fecha_cita'], errors='coerce')
    df = df[df['fecha_cita'].notna()]
    
    df = df[df['estado'] == 'CONFIRMADA']
    df = df[df['costo'] > 0]
    
    df['telefono'] = df['telefono'].fillna('NO REGISTRA')
    
    df.to_csv('data/output.csv', index=False)


if __name__ == "__main__":
    run_etl()
