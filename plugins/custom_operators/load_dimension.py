from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 table_name="",
                 column_name="",
                 expected_value=None,
                 *args, **kwargs):

        # Llamada al constructor de la clase base
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # Asignación de parámetros a atributos de la clase
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.column_name = column_name
        self.expected_value = expected_value

    def execute(self, context):
        # Lógica de validación de calidad de datos
        self.log.info(f"Verificando la calidad de los datos en la tabla {self.table_name} y columna {self.column_name}")

        # Conexión a la base de datos PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # Construcción de la consulta SQL para verificar la calidad de los datos
        query = f"""
            SELECT COUNT(*) 
            FROM {self.table_name} 
            WHERE {self.column_name} = %s
        """
        
        cursor.execute(query, (self.expected_value,))
        result = cursor.fetchone()

        # Si el resultado no es el esperado, lanzar una excepción
        if result[0] != 0:
            raise ValueError(f"La calidad de los datos no es la esperada. Esperado: 0, Obtenido: {result[0]}")
        
        self.log.info(f"Calidad de datos verificada con éxito. {result[0]} registros encontrados.")
