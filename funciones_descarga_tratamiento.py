from sqlalchemy import create_engine, text
import pandas as pd

################################################################
def descarga_eventos(liga):
    # Conexión con la base de datos
    url = f'postgresql://postgres:sergio9494@localhost:5432/Futbol24-25'
    consulta = f"SELECT id_jugador,equipo,jugador,es_toque,accion,resultado,resultado2,zona_campo,zona_contacto,situacion,tipo,zona_destino_pase,x,y FROM \"eventos\" INNER JOIN \"partidos\" ON eventos.id_partido = partidos.id_partido WHERE partidos.competicion = '{liga}'"
    engine = create_engine(url)
    resultado = engine.connect().execute((text(consulta)))
    df_eventos = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())

    return df_eventos # Eventos con los datos de la liga seleccionada

################################################################
def descarga_jugadores(liga):
    # Conexión con la base de datos
    url = f'postgresql://postgres:sergio9494@localhost:5432/Futbol24-25'
    consulta = f"SELECT id_jugador,nombre,competicion,equipo,titular,partido_jugado,minutos,posesion,goles_anotados,goles_recibidos,segundos_efectivo,segundos_posesion,segundos_defendiendo,capitan FROM \"jugadores\" INNER JOIN \"partidos\" ON jugadores.id_partido = partidos.id_partido WHERE partidos.competicion = '{liga}'"
    engine = create_engine(url)
    resultado = engine.connect().execute((text(consulta)))
    df_jugadores = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())

    return df_jugadores # Jugadores de la liga seleccionada

################################################################
def tratar_jugadores(df_jugadores):
    df_jugadores = df_jugadores.groupby(['id_jugador','nombre','competicion','equipo'], as_index=False).sum()
    return df_jugadores

################################################################
def tratar_eventos(df_eventos):
    df_eventos.loc[(df_eventos.resultado == 'Entrada fallida') & (df_eventos.es_toque == 'True'), 'resultado'] = 'Entrada exitosa'

    df_eventos.loc[df_eventos.zona_destino_pase == 'Derecha atras', 'zona_destino_pase'] = 'Atras'
    df_eventos.loc[df_eventos.zona_destino_pase == 'Izquierda atras', 'zona_destino_pase'] = 'Atras'
    df_eventos.loc[df_eventos.zona_destino_pase == 'atras', 'zona_destino_pase'] = 'Atras'

    df_eventos.loc[df_eventos.zona_destino_pase == 'Derecha adelante', 'zona_destino_pase'] = 'Adelante'
    df_eventos.loc[df_eventos.zona_destino_pase == 'Izquierda adelante', 'zona_destino_pase'] = 'Adelante'
    df_eventos.loc[df_eventos.zona_destino_pase == 'adelante', 'zona_destino_pase'] = 'Adelante'

    df_eventos.loc[df_eventos.zona_destino_pase == 'Derecha', 'zona_destino_pase'] = 'Horizontal'
    df_eventos.loc[df_eventos.zona_destino_pase == 'Izquierda', 'zona_destino_pase'] = 'Horizontal'

    df_eventos = df_eventos.drop(df_eventos[(df_eventos['accion'] == 'Final primera parte')].index).reset_index(drop=True)
    df_eventos = df_eventos.drop(df_eventos[(df_eventos['accion'] == 'Final segunda parte')].index).reset_index(drop=True)
    df_eventos = df_eventos.drop(df_eventos[(df_eventos['accion'] == 'FormationChange')].index).reset_index(drop=True)

    df_eventos['accion_resultado_zona'] = df_eventos.accion + '_' + df_eventos.resultado + '_' + df_eventos.zona_campo + '_' + df_eventos.zona_destino_pase + '_' + df_eventos.situacion
    #df_eventos['accion_resultado'] = df_eventos.accion + '_' + df_eventos.resultado
    df_acciones = df_eventos[['id_jugador','accion_resultado_zona']]
    columnas_acciones = {
            'accion_resultado_zona': {
        },
    }
    for columna, valor in columnas_acciones.items():
	    # Recogemos un DataFrame con una columna dummy por cada valor
        dummies = df_acciones[columna].str.get_dummies()
    
        # Añadimos las columnas dummy al df original
        df_acciones = pd.concat([df_acciones, dummies], axis=1)

    df_acciones = df_acciones.drop(['accion_resultado_zona'], axis=1)
    df_acciones = df_acciones.groupby(['id_jugador'], as_index=False).sum()

    return df_acciones

################################################################
def combinar_jugadores_acciones(df_jugadores, df_acciones):
    df_jugadores_final = pd.merge(df_jugadores, df_acciones, how='left', on='id_jugador').reset_index(drop=True)
    return df_jugadores_final

################################################################
def obtener_datos_tratados_ligas(ligas):
    df_jugadores_final = pd.DataFrame()
    for liga in ligas:
        df_eventos = descarga_eventos(liga)
        df_eventos = tratar_eventos(df_eventos)

        df_jugadores = descarga_jugadores(liga)
        df_jugadores = tratar_jugadores(df_jugadores)

        df_jugadores_acciones = combinar_jugadores_acciones(df_jugadores, df_eventos)
        df_jugadores_final = pd.concat([df_jugadores_final, df_jugadores_acciones], axis=0).reset_index(drop=True)
    df_jugadores_final = df_jugadores_final.fillna(0)
    return df_jugadores_final

