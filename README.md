# 311 NYC Service Request Web-Service
***
<div align="justify">

### Integrantes:

- Cadavid Sánchez Sebastián, [C1587S](https://github.com/C1587S)
- Herrera Musi Juan Pablo, [Pilo1961](https://github.com/Pilo1961)
- Paz Cendejas Francisco, [MrFranciscoPaz](https://github.com/MrFranciscoPaz)
- Villa Lizárraga Diego M., [dvilla88](https://github.com/dvilla88)
- Pinto Veizaga Daniela, [dapivei](https://github.com/dapivei)


### Predicción:

Tiempo estimado de resolución de un *service request*

### Variables:



### Data Product Architecture:

```mermaid
graph LR
    id1(API NYC 311) -->
    id2(Extract)-. S3 .->
    id3(Transform ETL)-->
    id4[(Database)]-->
    id5(Transform ML) -->
    id6(Módulos Spark/Training)-->
    id7(Predecir)-->
    id8((Web Server))

```

<p align="center">
<image width="800" height="90" src="https://github.com/dapivei/data-product-architecture-final-project/blob/master/images/mockup.png">
</p>

### Producto Final:
</div>

<p align="center">
  <image width="400" height="300" src="https://github.com/dapivei/data-product-architecture-final-project/blob/master/images/web_service_proposal.png">
</p>
