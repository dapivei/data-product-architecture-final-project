#!python3

from flask import Flask
from flask_restplus import Api, Resource
from flask import request
import psycopg2 as ps
from dynaconf import settings

app = Flask(__name__)
api = Api(app)
ns = api.namespace('API-NYC311',
                   description='Predicciones de dias con demanda atipica')



@ns.route('/info_modelo', endpoint='endpoint_info_modelo')
class Modelo(Resource):
    """Aquí debe ir la descripción de la clase Modelo
    Aquí debe ir el segundo renglón
    Y aquí debe ir el tercer renglón
    """
    parser = ns.parser()
    parser.add_argument('anio', required=True, type=int, help='Año de la predicción', nullable=True)
    parser.add_argument('mes', required=True, type=int, help='Mes de la predicción', choices=(1,2,3,4), default=1, nullable=True)
    parser.add_argument('dia', required=True, type=int, help='Día de la predicción', nullable=True)

    @ns.expect(parser, validate=True)
    def get(self):
        anio = str(request.args.get('anio'))
        mes = str(request.args.get('mes'))
        dia = str(request.args.get('dia'))

        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        sql = f"SELECT * FROM modeling.ejecucion WHERE param_year='{anio}' and param_month='{mes}' and param_day='{dia}';"
        cur.execute(sql)
        data=cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return  data


@ns.route("/predict")
class Predict(Resource):

    #obj_Rita = Rita()
    #meses = obj_Rita.ObtenerMeses()

    parser = ns.parser()
    parser.add_argument('anio', required=True, type=int, help='Año de la predicción', nullable=True)
    parser.add_argument('mes', required=True, type=int, help='Mes de la predicción', choices=(1,2,3,4), default=1, nullable=True)
    parser.add_argument('dia', required=True, type=int, help='Día de la predicción', nullable=True)

    @ns.expect(parser, validate=False)
    def get(self):
        anio = request.args.get('anio')
        mes = request.args.get('mes')
        dia = request.args.get('dia')
        return {'Anio': anio, 'mes': mes, 'dia':dia}


if __name__ == '__main__':

    app.run(host='0.0.0.0', debug = False)
    #app.run()
