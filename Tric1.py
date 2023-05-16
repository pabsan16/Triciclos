# -*- coding: utf-8 -*-
"""
PRIMER APARTADO

PABLO SÁNCHEZ RICO
"""

import sys
from pyspark import SparkContext


def mapp(line):
    nodes = line.split(' , ')
    if nodes[0] < nodes[1]:
        return (nodes[0], nodes[1])
    elif nodes[1] < nodes[0]:
        return (nodes[1], nodes[0])
    else:
        pass
    
    
def adj_list(nlist):
    node = nlist[0]
    l = nlist[1]
    m = len(l)
    ad_list = []
    for e in range(m):
        a = l[e]
        ad_list.append(((node, a), 'exists'))
        for k in range(e+1, m):
            ad_list.append(((a, l[k]), ('pending', node)))
    return ad_list


def main(sc, filename):
    """
    Primero leemos el fichero, creamos el grafo mapeando
    y eliminando duplicados. Después creamos la lista de nodos
    adyacentes ordenando por Key, obtenemos los triciclos y 
    luego imprimimos la lista resultante.
    """
    g = sc.textFile(filename)
    gra = g.map(mapp).distinct().filter(lambda x: x!=None)
    ady = gra.groupByKey().map(lambda x: (x[0], sorted(list(x[1])))).sortByKey()
    tric = ady.flatMap(adj_list).groupByKey()
    list_tric = []
    for par, msn in tric.collect():
        list_msn = list(msn)
        if len(list_msn) > 1 and 'exists' in list_msn:
            for i in list_msn:
                if i != 'exists':
                    list_tric.append((i[1], par[0], par[1]))
    print ('Trycicle List: ', sorted(list_tric))
    
    
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Uso: python3 {0} <file>'.format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel('ERROR')
            main(sc, sys.argv[1])
    
