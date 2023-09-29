# Press the green button in the gutter to run the script.

import configparser
import datetime
import logging
import logging.handlers
import subprocess
import sys
import threading
import time
import traceback

_logger = logging.getLogger()
logging.basicConfig(
    filename='supervisor.log',
    format='%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s',
    level=logging.INFO
)


def worker(worker_parameters):
    process_name = worker_parameters[0]
    config_parameters = worker_parameters[1]
    program_name = config_parameters['program_name']
    desde = config_parameters.get('desde')
    hasta = config_parameters.get('hasta')
    una_vez = config_parameters.get('una_vez', '').lower() == 'si'

    args = []
    if 'arg1' in config_parameters:
        args.append(config_parameters['arg1'])
    if 'arg2' in config_parameters:
        args.append(config_parameters['arg2'])
    if 'arg3' in config_parameters:
        args.append(config_parameters['arg3'])
    if 'arg4' in config_parameters:
        args.append(config_parameters['arg4'])
    if 'arg5' in config_parameters:
        args.append(config_parameters['arg5'])

    def parse_hora(hora_str):
        components = hora_str.split(':')
        if len(components) != 2 or not components[0] or not components[1]:
            raise ValueError(f'La hora tiene un formato inválido, no es HH:MM. Valor ingresado {hora_str}')
        hora = int(components[0])
        minutos = int(components[1])
        if hora < 0 or hora >= 24:
            raise ValueError('El valor de la hora es inválido! Debe ser entre 0 y 23 inclusive')
        if minutos < 0 or hora >= 60:
            raise ValueError('El valor de los minutos es inválido! Debe ser entre 0 y 59 inclusive')

        return hora, minutos

    while True:
        now = datetime.datetime.now()
        if desde:
            desde_hora, desde_minutos = parse_hora(desde)
            desde_dt = datetime.datetime(
                year=now.year,
                month=now.month,
                day=now.day,
                hour=desde_hora,
                minute=desde_minutos,
                second=0,
            )

            if hasta:
                hasta_hora, hasta_minutos = parse_hora(hasta)
                hasta_dt = datetime.datetime(
                    year=now.year,
                    month=now.month,
                    day=now.day,
                    hour=hasta_hora,
                    minute=hasta_minutos,
                    second=0,
                )

        if desde and now < desde_dt:
            time.sleep(20)
            continue

        if desde and hasta and now > hasta_dt:
            time.sleep(20)
            continue

        try:
            _logger.info(f'Arrancando proceso {process_name}')
            subprocess.run([program_name] + args, check=True)
            _logger.info(f'Proceso terminado sin error {process_name}')

            if una_vez:
                break

            # Espera para no saturar en caso de procesos con error
            time.sleep(10)

        except subprocess.CalledProcessError as e:
            _logger.error(f'Excepción inesperada {process_name} - {e}')


if __name__ == '__main__':

    try:
        # Leer archivo de
        _logger.info('Leyendo archivo de configuración supervisor.config')
        config = configparser.ConfigParser()
        config.read('supervisor.config')

        if len(config.sections()) == 0:
            _logger.info('NO se ha configurado ningún programa a supervisar!!. Por favor revisar')
            exit(0)

        _logger.info(f'Creando threads de ejecución de procesos ({len(config.sections())} proceso(s))')
        threads = []
        for pname in config.sections():
            keys = config[pname].keys()
            thread = threading.Thread(target=worker, args=[(pname, {k: config[pname][k] for k in keys})])
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        exit(0)

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        exc_string = ''
        for line in traceback.format_tb(exc_traceback):
            exc_string += line

        _logger.error(f'Excepción inesperada en supervisor\n{exc_string}')

        sys.stderr.write('EXCEPCION INESPERADA EN SUPERVISOR:\n')
        sys.stderr.write(exc_string)

        exit(1)
