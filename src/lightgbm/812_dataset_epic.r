#source("~/buckets/b1/crudoB/R/812_dataset_epic.r")
#Necesita para correr en Google Cloud
#256 GB de memoria RAM
#300 GB de espacio en el disco local
#8 vCPU


#limpio la memoria
rm( list=ls() )  #remove all objects
gc()             #garbage collection

require("data.table")
require("Rcpp")
require("rlist")
require("yaml")

require("lightgbm")


#defino la carpeta donde trabajo
directory.root  <-  "~/buckets/b1/"  #Google Cloud
#setwd( directory.root )
setwd( "/media/juan/Extraible/Maestria/EyF/" )


palancas  <- list()  #variable con las palancas para activar/desactivar

palancas$version  <- "v009"   #Muy importante, ir cambiando la version

#v008 es con la FE generadas por el primer MODELO

palancas$variablesdrift  <- c("ccajas_transacciones", "Master_mpagominimo", "internet", "mcaja_ahorro_dolares", "mpayroll", "matm_other", "tmobile_app", "cmobile_app_trx"  )   #aqui van las columnas que se quieren eliminar

palancas$corregir <-  TRUE    # TRUE o FALSE

palancas$nuevasvars <-  TRUE  #si quiero hacer Feature Engineering manual

palancas$dummiesNA  <-  FALSE #Idea de Santiago Dellachiesa de UAustral

palancas$lag1   <- FALSE    #lag de orden 1
palancas$delta1 <- FALSE    # campo -  lag de orden 1 
palancas$lag2   <- FALSE
palancas$delta2 <- FALSE
palancas$lag3   <- FALSE
palancas$delta3 <- FALSE
palancas$lag4   <- FALSE
palancas$delta4 <- FALSE
palancas$lag5   <- FALSE
palancas$delta5 <- FALSE
palancas$lag6   <- FALSE
palancas$delta6 <- FALSE

palancas$promedio3  <- FALSE  #promedio  de los ultimos 3 meses
palancas$promedio6  <- FALSE

palancas$minimo3  <- FALSE  #minimo de los ultimos 3 meses
palancas$minimo6  <- FALSE

palancas$maximo3  <- FALSE  #maximo de los ultimos 3 meses
palancas$maximo6  <- FALSE

palancas$tendencia6  <- FALSE    #Great power comes with great responsability


palancas$canaritosimportancia  <- FALSE  #si me quedo solo con lo mas importante de canaritosimportancia


#escribo para saber cuales fueron los parametros
write_yaml(  palancas,  paste0( "./work/palanca_",  palancas$version  ,".yaml" ) )

#------------------------------------------------------------------------------

ReportarCampos  <- function( dataset )
{
  cat( "La cantidad de campos es ", ncol(dataset) , "\n" )
}
#------------------------------------------------------------------------------
#Agrega al dataset una variable que va de 1 a 12, el mes, para que el modelo aprenda estacionalidad

AgregarMes  <- function( dataset )
{
  dataset[  , mes := foto_mes %% 100 ]
  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#Elimina las variables que uno supone hace Data Drifting

DriftEliminar  <- function( dataset, variables )
{
  dataset[  , c(variables) := NULL ]
  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#A las variables que tienen nulos, les agrega una nueva variable el dummy de is es nulo o no {0, 1}

DummiesNA  <- function( dataset )
{

  nulos  <- colSums( is.na(dataset[foto_mes==202101]) )  #cuento la cantidad de nulos por columna
  colsconNA  <- names( which(  nulos > 0 ) )

  dataset[ , paste0( colsconNA, "_isNA") :=  lapply( .SD,  is.na ),
             .SDcols= colsconNA]

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#Corrige poniendo a NA las variables que en ese mes estan dañadas

Corregir  <- function( dataset )
{
  #acomodo los errores del dataset

  dataset[ foto_mes==201801,  internet   := NA ]
  dataset[ foto_mes==201801,  thomebanking   := NA ]
  dataset[ foto_mes==201801,  chomebanking_transacciones   := NA ]
  dataset[ foto_mes==201801,  tcallcenter   := NA ]
  dataset[ foto_mes==201801,  ccallcenter_transacciones   := NA ]
  dataset[ foto_mes==201801,  cprestamos_personales   := NA ]
  dataset[ foto_mes==201801,  mprestamos_personales   := NA ]
  dataset[ foto_mes==201801,  mprestamos_hipotecarios  := NA ]
  dataset[ foto_mes==201801,  ccajas_transacciones   := NA ]
  dataset[ foto_mes==201801,  ccajas_consultas   := NA ]
  dataset[ foto_mes==201801,  ccajas_depositos   := NA ]
  dataset[ foto_mes==201801,  ccajas_extracciones   := NA ]
  dataset[ foto_mes==201801,  ccajas_otras   := NA ]

  dataset[ foto_mes==201806,  tcallcenter   :=  NA ]
  dataset[ foto_mes==201806,  ccallcenter_transacciones   :=  NA ]

  dataset[ foto_mes==201904,  ctarjeta_visa_debitos_automaticos  :=  NA ]
  dataset[ foto_mes==201904,  mttarjeta_visa_debitos_automaticos := NA ]
  dataset[ foto_mes==201904,  Visa_mfinanciacion_limite := NA ]

  dataset[ foto_mes==201905,  mrentabilidad     := NA ]
  dataset[ foto_mes==201905,  mrentabilidad_annual     := NA ]
  dataset[ foto_mes==201905,  mcomisiones      := NA ]
  dataset[ foto_mes==201905,  mpasivos_margen  := NA ]
  dataset[ foto_mes==201905,  mactivos_margen  := NA ]
  dataset[ foto_mes==201905,  ctarjeta_visa_debitos_automaticos  := NA ]
  dataset[ foto_mes==201905,  ccomisiones_otras := NA ]
  dataset[ foto_mes==201905,  mcomisiones_otras := NA ]

  dataset[ foto_mes==201910,  mpasivos_margen   := NA ]
  dataset[ foto_mes==201910,  mactivos_margen   := NA ]
  dataset[ foto_mes==201910,  ccomisiones_otras := NA ]
  dataset[ foto_mes==201910,  mcomisiones_otras := NA ]
  dataset[ foto_mes==201910,  mcomisiones       := NA ]
  dataset[ foto_mes==201910,  mrentabilidad     := NA ]
  dataset[ foto_mes==201910,  mrentabilidad_annual        := NA ]
  dataset[ foto_mes==201910,  chomebanking_transacciones  := NA ]
  dataset[ foto_mes==201910,  ctarjeta_visa_descuentos    := NA ]
  dataset[ foto_mes==201910,  ctarjeta_master_descuentos  := NA ]
  dataset[ foto_mes==201910,  mtarjeta_visa_descuentos    := NA ]
  dataset[ foto_mes==201910,  mtarjeta_master_descuentos  := NA ]
  dataset[ foto_mes==201910,  ccajeros_propios_descuentos := NA ]
  dataset[ foto_mes==201910,  mcajeros_propios_descuentos := NA ]

  dataset[ foto_mes==202001,  cliente_vip   := NA ]

  dataset[ foto_mes==202006,  active_quarter   := NA ]
  dataset[ foto_mes==202006,  internet   := NA ]
  dataset[ foto_mes==202006,  mrentabilidad   := NA ]
  dataset[ foto_mes==202006,  mrentabilidad_annual   := NA ]
  dataset[ foto_mes==202006,  mcomisiones   := NA ]
  dataset[ foto_mes==202006,  mactivos_margen   := NA ]
  dataset[ foto_mes==202006,  mpasivos_margen   := NA ]
  dataset[ foto_mes==202006,  mcuentas_saldo   := NA ]
  dataset[ foto_mes==202006,  ctarjeta_debito_transacciones   := NA ]
  dataset[ foto_mes==202006,  mautoservicio   := NA ]
  dataset[ foto_mes==202006,  ctarjeta_visa_transacciones   := NA ]
  dataset[ foto_mes==202006,  mtarjeta_visa_consumo   := NA ]
  dataset[ foto_mes==202006,  ctarjeta_master_transacciones   := NA ]
  dataset[ foto_mes==202006,  mtarjeta_master_consumo   := NA ]
  dataset[ foto_mes==202006,  ccomisiones_otras   := NA ]
  dataset[ foto_mes==202006,  mcomisiones_otras   := NA ]
  dataset[ foto_mes==202006,  cextraccion_autoservicio   := NA ]
  dataset[ foto_mes==202006,  mextraccion_autoservicio   := NA ]
  dataset[ foto_mes==202006,  ccheques_depositados   := NA ]
  dataset[ foto_mes==202006,  mcheques_depositados   := NA ]
  dataset[ foto_mes==202006,  ccheques_emitidos   := NA ]
  dataset[ foto_mes==202006,  mcheques_emitidos   := NA ]
  dataset[ foto_mes==202006,  ccheques_depositados_rechazados   := NA ]
  dataset[ foto_mes==202006,  mcheques_depositados_rechazados   := NA ]
  dataset[ foto_mes==202006,  ccheques_emitidos_rechazados   := NA ]
  dataset[ foto_mes==202006,  mcheques_emitidos_rechazados   := NA ]
  dataset[ foto_mes==202006,  tcallcenter   := NA ]
  dataset[ foto_mes==202006,  ccallcenter_transacciones   := NA ]
  dataset[ foto_mes==202006,  thomebanking   := NA ]
  dataset[ foto_mes==202006,  chomebanking_transacciones   := NA ]
  dataset[ foto_mes==202006,  ccajas_transacciones   := NA ]
  dataset[ foto_mes==202006,  ccajas_consultas   := NA ]
  dataset[ foto_mes==202006,  ccajas_depositos   := NA ]
  dataset[ foto_mes==202006,  ccajas_extracciones   := NA ]
  dataset[ foto_mes==202006,  ccajas_otras   := NA ]
  dataset[ foto_mes==202006,  catm_trx   := NA ]
  dataset[ foto_mes==202006,  matm   := NA ]
  dataset[ foto_mes==202006,  catm_trx_other   := NA ]
  dataset[ foto_mes==202006,  matm_other   := NA ]
  dataset[ foto_mes==202006,  ctrx_quarter   := NA ]
  dataset[ foto_mes==202006,  tmobile_app   := NA ]
  dataset[ foto_mes==202006,  cmobile_app_trx   := NA ]


  dataset[ foto_mes==202010,  internet  := NA ]
  dataset[ foto_mes==202011,  internet  := NA ]
  dataset[ foto_mes==202012,  internet  := NA ]
  dataset[ foto_mes==202101,  internet  := NA ]

  dataset[ foto_mes==202009,  tmobile_app  := NA ]
  dataset[ foto_mes==202010,  tmobile_app  := NA ]
  dataset[ foto_mes==202011,  tmobile_app  := NA ]
  dataset[ foto_mes==202012,  tmobile_app  := NA ]
  dataset[ foto_mes==202101,  tmobile_app  := NA ]

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#Esta es la parte que los alumnos deben desplegar todo su ingenio

AgregarVariables  <- function( dataset )
{
  #INICIO de la seccion donde se deben hacer cambios con variables nuevas
  #se crean los nuevos campos para MasterCard  y Visa, teniendo en cuenta los NA's
  #varias formas de combinar Visa_status y Master_status
  dataset[ , mv_status01       := pmax( Master_status,  Visa_status, na.rm = TRUE) ]
  dataset[ , mv_status02       := Master_status +  Visa_status ]
  dataset[ , mv_status03       := pmax( ifelse( is.na(Master_status), 10, Master_status) , ifelse( is.na(Visa_status), 10, Visa_status) ) ]
  dataset[ , mv_status04       := ifelse( is.na(Master_status), 10, Master_status)  +  ifelse( is.na(Visa_status), 10, Visa_status)  ]
  dataset[ , mv_status05       := ifelse( is.na(Master_status), 10, Master_status)  +  100*ifelse( is.na(Visa_status), 10, Visa_status)  ]

  dataset[ , mv_status06       := ifelse( is.na(Visa_status), 
                                          ifelse( is.na(Master_status), 10, Master_status), 
                                          Visa_status)  ]

  dataset[ , mv_status07       := ifelse( is.na(Master_status), 
                                          ifelse( is.na(Visa_status), 10, Visa_status), 
                                          Master_status)  ]


  #combino MasterCard y Visa
  dataset[ , mv_mfinanciacion_limite := rowSums( cbind( Master_mfinanciacion_limite,  Visa_mfinanciacion_limite) , na.rm=TRUE ) ]

  dataset[ , mv_Fvencimiento         := pmin( Master_Fvencimiento, Visa_Fvencimiento, na.rm = TRUE) ]
  dataset[ , mv_Finiciomora          := pmin( Master_Finiciomora, Visa_Finiciomora, na.rm = TRUE) ]
  dataset[ , mv_msaldototal          := rowSums( cbind( Master_msaldototal,  Visa_msaldototal) , na.rm=TRUE ) ]
  dataset[ , mv_msaldopesos          := rowSums( cbind( Master_msaldopesos,  Visa_msaldopesos) , na.rm=TRUE ) ]
  dataset[ , mv_msaldodolares        := rowSums( cbind( Master_msaldodolares,  Visa_msaldodolares) , na.rm=TRUE ) ]
  dataset[ , mv_mconsumospesos       := rowSums( cbind( Master_mconsumospesos,  Visa_mconsumospesos) , na.rm=TRUE ) ]
  dataset[ , mv_mconsumosdolares     := rowSums( cbind( Master_mconsumosdolares,  Visa_mconsumosdolares) , na.rm=TRUE ) ]
  dataset[ , mv_mlimitecompra        := rowSums( cbind( Master_mlimitecompra,  Visa_mlimitecompra) , na.rm=TRUE ) ]
  dataset[ , mv_madelantopesos       := rowSums( cbind( Master_madelantopesos,  Visa_madelantopesos) , na.rm=TRUE ) ]
  dataset[ , mv_madelantodolares     := rowSums( cbind( Master_madelantodolares,  Visa_madelantodolares) , na.rm=TRUE ) ]
  dataset[ , mv_fultimo_cierre       := pmax( Master_fultimo_cierre, Visa_fultimo_cierre, na.rm = TRUE) ]
  dataset[ , mv_mpagado              := rowSums( cbind( Master_mpagado,  Visa_mpagado) , na.rm=TRUE ) ]
  dataset[ , mv_mpagospesos          := rowSums( cbind( Master_mpagospesos,  Visa_mpagospesos) , na.rm=TRUE ) ]
  dataset[ , mv_mpagosdolares        := rowSums( cbind( Master_mpagosdolares,  Visa_mpagosdolares) , na.rm=TRUE ) ]
  dataset[ , mv_fechaalta            := pmax( Master_fechaalta, Visa_fechaalta, na.rm = TRUE) ]
  dataset[ , mv_mconsumototal        := rowSums( cbind( Master_mconsumototal,  Visa_mconsumototal) , na.rm=TRUE ) ]
  dataset[ , mv_cconsumos            := rowSums( cbind( Master_cconsumos,  Visa_cconsumos) , na.rm=TRUE ) ]
  dataset[ , mv_cadelantosefectivo   := rowSums( cbind( Master_cadelantosefectivo,  Visa_cadelantosefectivo) , na.rm=TRUE ) ]
  #dataset[ , mv_mpagominimo          := rowSums( cbind( Master_mpagominimo,  Visa_mpagominimo) , na.rm=TRUE ) ]

  #a partir de aqui juego con la suma de Mastercard y Visa
  dataset[ , mvr_Master_mlimitecompra:= Master_mlimitecompra / mv_mlimitecompra ]
  dataset[ , mvr_Visa_mlimitecompra  := Visa_mlimitecompra / mv_mlimitecompra ]
  dataset[ , mvr_msaldototal         := mv_msaldototal / mv_mlimitecompra ]
  dataset[ , mvr_msaldopesos         := mv_msaldopesos / mv_mlimitecompra ]
  dataset[ , mvr_msaldopesos2        := mv_msaldopesos / mv_msaldototal ]
  dataset[ , mvr_msaldodolares       := mv_msaldodolares / mv_mlimitecompra ]
  dataset[ , mvr_msaldodolares2      := mv_msaldodolares / mv_msaldototal ]
  dataset[ , mvr_mconsumospesos      := mv_mconsumospesos / mv_mlimitecompra ]
  dataset[ , mvr_mconsumosdolares    := mv_mconsumosdolares / mv_mlimitecompra ]
  dataset[ , mvr_madelantopesos      := mv_madelantopesos / mv_mlimitecompra ]
  dataset[ , mvr_madelantodolares    := mv_madelantodolares / mv_mlimitecompra ]
  dataset[ , mvr_mpagado             := mv_mpagado / mv_mlimitecompra ]
  dataset[ , mvr_mpagospesos         := mv_mpagospesos / mv_mlimitecompra ]
  dataset[ , mvr_mpagosdolares       := mv_mpagosdolares / mv_mlimitecompra ]
  dataset[ , mvr_mconsumototal       := mv_mconsumototal  / mv_mlimitecompra ]
  #dataset[ , mvr_mpagominimo         := mv_mpagominimo  / mv_mlimitecompra ]

  #Aqui debe usted agregar sus propias nuevas variables

  #VARIABLES QUE INTENTO CREAR YO
  
  #CONSUMO TARJETAS / SALDO PF
  
  dataset[ , V1                      :=(Master_mconsumototal+Visa_mconsumototal)/mplazo_fijo_pesos]
  
  #CONSUMO TARJETAS / SALDO en Cuentas convertido todo a $
  
  dataset[ , V2                      :=(Master_mconsumototal+Visa_mconsumototal)/mcuentas_saldo]
  
  # PAGO SUELDO / LIMITE TARJETA. Si cobro mucho puede que sea indemnización y tal vez luego de cobrar eso se vaya del banco
  #V3 con Master
  #dataset[ , V3                       :=mpayroll/Master_mlimitecompra]
  #V4 con Visa
  #dataset[ , V4                       :=mpayroll/Visa_mlimitecompra]
  
  #EDAD con Saldo caja de Ahorro
  
  dataset[ , V5                       := mcaja_ahorro/cliente_edad]
  
  #Antiguedad cliente con Saldo
  
  dataset[ , V6                        :=mcaja_ahorro/(cliente_antiguedad/12)]
  
  #USO AHORA ALGUNAS DE LAS QUE OBTUVE COMO MÁS IMPORTANTES
  
  #dataset[ , V7                        :=(mpayroll+mpayroll2)/cpayroll_trx]
  
  dataset[ , V8                        :=mtarjeta_visa_consumo/ctarjeta_visa_transacciones ]
  
  dataset[ , V9                        :=mtarjeta_visa_consumo/(mcomisiones_otras+mcomisiones_mantenimiento) ]
  
  dataset[ , V9b                        :=1/V9 ]
  
  
  dataset[ , V10                       :=mtarjeta_visa_consumo/(ccomisiones_mantenimiento+ccomisiones_otras) ]
  
  dataset[ , V11                       :=mtarjeta_visa_consumo/Visa_msaldopesos ]
  
  dataset[ , V11b                       :=1/V11 ]
  
  
  dataset[ , V12                       :=ctarjeta_visa_transacciones/(ccomisiones_mantenimiento+ccomisiones_otras) ]
  
  dataset[ , V13                       :=1/(ctarjeta_visa_transacciones/Visa_msaldopesos)]
  
  dataset[ , V14                       :=1/(ctarjeta_visa_transacciones/(mcomisiones_otras+mcomisiones_mantenimiento))]
  
  dataset[ , V15                       :=(mcomisiones_mantenimiento+mcomisiones_otras)/(ccomisiones_mantenimiento+ccomisiones_otras)]
  
  dataset[ , V16                       :=mtarjeta_visa_consumo/cpayroll_trx]
  
  dataset[ , V17                       :=mcuentas_saldo/cpayroll_trx]
  
  dataset[ , V18                       :=Visa_msaldototal/cpayroll_trx]
  
  dataset[ , V19                       :=mprestamos_personales/cpayroll_trx]
  
  dataset[ , V20                       :=mcuenta_corriente/cpayroll_trx]
  
  dataset[ , V21                       :=mpasivos_margen/cpayroll_trx]
  
  dataset[ , V22                       :=Visa_msaldopesos/cpayroll_trx]
  
  dataset[ , V23                       :=mrentabilidad_annual/cpayroll_trx]
  
  dataset[ , V24                       :=mcomisiones_mantenimiento/cpayroll_trx]
  
  dataset[ , V25                       :=mrentabilidad/cpayroll_trx]
  
  dataset[ , V26                       :=mcomisiones_otras/cpayroll_trx]
  
  dataset[ , V27                       :=mcomisiones/cpayroll_trx]
  
  dataset[ , V28                       :=ccomisiones_otras/cpayroll_trx]
  
  dataset[ , V29                       :=ccomisiones_mantenimiento/cpayroll_trx]
  
  dataset[ , V30                       :=mautoservicio/cpayroll_trx]
  
  dataset[ , V31                       :=ctarjeta_debito_transacciones/cpayroll_trx]
  
  dataset[ , V32                       :=Visa_mpagominimo/cpayroll_trx]
  
  dataset[ , V33                       :=mtarjeta_visa_consumo/mcuentas_saldo]
  
  dataset[ , V33b                       :=1/(mtarjeta_visa_consumo/mcuentas_saldo)]
  
  dataset[ , V34                       :=mtarjeta_visa_consumo/Visa_msaldototal]
  
  dataset[ , V34b                       :=1/(mtarjeta_visa_consumo/Visa_msaldototal)]
  
  dataset[ , V35                       :=mtarjeta_visa_consumo/mprestamos_personales]
  
  dataset[ , V35b                       :=1/(mtarjeta_visa_consumo/mprestamos_personales)]
  
  dataset[ , V36                       :=mtarjeta_visa_consumo/mcuenta_corriente]
  
  dataset[ , V36b                       :=1/V36]
  
  dataset[ , V37                       :=mtarjeta_visa_consumo/mpasivos_margen]
  
  dataset[ , V37b                       :=1/V37]
  
  
  dataset[ , V38                       :=mtarjeta_visa_consumo/Visa_msaldopesos]
  
  dataset[ , V38b                       :=1/V38]
  
  dataset[ , V39                       :=mtarjeta_visa_consumo/mrentabilidad_annual]
  
  dataset[ , V39b                       :=1/V39]
  
  dataset[ , V40                      :=mtarjeta_visa_consumo/mcomisiones_mantenimiento]
  
  dataset[ , V40b                      :=1/V40]
  
  dataset[ , V41                      :=mtarjeta_visa_consumo/mrentabilidad]
  
  dataset[ , V41b                      :=1/V41]
  
  dataset[ , V42                      :=mtarjeta_visa_consumo/mcomisiones_otras]
  
  dataset[ , V42b                      :=1/V42]
  
  dataset[ , V43                      :=mtarjeta_visa_consumo/mcomisiones]
  
  dataset[ , V43b                      :=1/V43]
  
  dataset[ , V44                      :=mtarjeta_visa_consumo/mautoservicio]
  
  dataset[ , V44b                      :=1/V44]
  
  dataset[ , V45                      :=mtarjeta_visa_consumo/Visa_mpagominimo]
  
  dataset[ , V45b                      :=1/V45]
  
  dataset[ , V46                      :=mcuentas_saldo/ctarjeta_visa_transacciones]
  
  dataset[ , V46b                      :=1/V46]
  
  dataset[ , V47                      :=mcuentas_saldo/Visa_msaldototal]
  
  dataset[ , V47b                      :=1/V47]
  
  dataset[ , V48                      :=mcuentas_saldo/mprestamos_personales]
  
  dataset[ , V48b                      :=1/V48]
  
  dataset[ , V49                      :=mcuentas_saldo/mcuenta_corriente]
  
  dataset[ , V49b                      :=1/V49]
  
  dataset[ , V50                      :=mcuentas_saldo/mpasivos_margen]
  
  dataset[ , V50b                      :=1/V50]
  
  dataset[ , V51                      :=mcuentas_saldo/Visa_msaldopesos]
  
  dataset[ , V51b                      :=1/V51]
  
  dataset[ , V52                      :=mcuentas_saldo/mrentabilidad_annual]
  
  dataset[ , V52b                      :=1/V52]
  
  dataset[ , V53                      :=mcuentas_saldo/mcomisiones_mantenimiento]
  
  dataset[ , V53b                      :=1/V53]
  
  dataset[ , V54                      :=mcuentas_saldo/mrentabilidad]
  
  dataset[ , V54b                      :=1/V54]
  
  dataset[ , V55                      :=mcuentas_saldo/mcomisiones_otras]
  
  dataset[ , V55b                      :=1/V55]
  
  dataset[ , V56                      :=mcuentas_saldo/mcomisiones]
  
  dataset[ , V56b                      :=1/V56]
  
  dataset[ , V57                      :=mcuentas_saldo/mautoservicio]
  
  dataset[ , V57b                      :=1/V57]
  
  dataset[ , V58                      :=mcuentas_saldo/Visa_mpagominimo]
  
  dataset[ , V58b                      :=1/V58]
  
  dataset[ , V59                      :=Visa_msaldototal/mprestamos_personales]
  
  dataset[ , V59b                      :=1/V59]
  
  dataset[ , V60                      :=Visa_msaldototal/mcuenta_corriente]
  
  dataset[ , V60b                      :=1/V60]
  
  dataset[ , V61                      :=Visa_msaldototal/mpasivos_margen]
  
  dataset[ , V61b                      :=1/V61]
  
  dataset[ , V62                      :=Visa_msaldototal/Visa_msaldopesos]
  
  dataset[ , V62b                      :=1/V62]
  
  dataset[ , V63                      :=Visa_msaldototal/mrentabilidad_annual]
  
  dataset[ , V63b                      :=1/V63]
  
  dataset[ , V64                      :=Visa_msaldototal/mcomisiones_mantenimiento]
  
  dataset[ , V64b                      :=1/V64]
  
  dataset[ , V65                      :=Visa_msaldototal/mrentabilidad]
  
  dataset[ , V65b                      :=1/V65]
  
  dataset[ , V66                      :=Visa_msaldototal/mcomisiones_otras]
  
  dataset[ , V66b                      :=1/V66]
  
  dataset[ , V67                      :=Visa_msaldototal/mcomisiones]
  
  dataset[ , V67b                      :=1/V67]
  
  dataset[ , V68                      :=Visa_msaldototal/mautoservicio]
  
  dataset[ , V68b                      :=1/V68]
  
  dataset[ , V69                      :=Visa_msaldototal/Visa_mpagominimo]
  
  dataset[ , V69b                      :=1/V69]
  
  dataset[ , V70                      :=mprestamos_personales/mcuenta_corriente]
  
  dataset[ , V70b                      :=1/V70]
  
  dataset[ , V71                      :=mprestamos_personales/mpasivos_margen]
  
  dataset[ , V71b                      :=1/V71]
  
  dataset[ , V72                      :=mprestamos_personales/Visa_msaldopesos]
  
  dataset[ , V72b                      :=1/V72]
  
  dataset[ , V73                      :=mprestamos_personales/mrentabilidad_annual]
  
  dataset[ , V73b                      :=1/V73]
  
  dataset[ , V74                      :=mprestamos_personales/mcomisiones_mantenimiento]
  
  dataset[ , V74b                      :=1/V74]
  
  dataset[ , V75                      :=mprestamos_personales/mrentabilidad]
  
  dataset[ , V75b                      :=1/V75]
  
  dataset[ , V76                      :=mprestamos_personales/mcomisiones_otras]
  
  dataset[ , V76b                      :=1/V76]
  
  dataset[ , V77                      :=mprestamos_personales/mcomisiones]
  
  dataset[ , V77b                      :=1/V77]
  
  dataset[ , V78                      :=mprestamos_personales/mautoservicio]
  
  dataset[ , V78b                      :=1/V78]
  
  dataset[ , V79                      :=mprestamos_personales/Visa_mpagominimo]
  
  dataset[ , V79b                      :=1/V79]
  
  dataset[ , V80                      :=mcuenta_corriente/mpasivos_margen]
  
  dataset[ , V80b                      :=1/V80]
  
  dataset[ , V81                      :=mcuenta_corriente/Visa_msaldopesos]
  
  dataset[ , V81b                      :=1/V81]
  
  dataset[ , V82                      :=mcuenta_corriente/mrentabilidad_annual]
  
  dataset[ , V82b                      :=1/V82]
  
  dataset[ , V83                      :=mcuenta_corriente/mcomisiones_mantenimiento]
  
  dataset[ , V83b                      :=1/V83]
  
  dataset[ , V84                      :=mcuenta_corriente/mrentabilidad]
  
  dataset[ , V84b                      :=1/V84]
  
  dataset[ , V85                      :=mcuenta_corriente/mcomisiones_otras]
  
  dataset[ , V85b                      :=1/V85]
  
  dataset[ , V86                      :=mcuenta_corriente/mcomisiones]
  
  dataset[ , V86b                      :=1/V86]
  
  dataset[ , V87                      :=mcuenta_corriente/mautoservicio]
  
  dataset[ , V87b                      :=1/V87]
  
  dataset[ , V87c                      :=mcuenta_corriente/Visa_mpagominimo]
  
  dataset[ , V87d                      :=1/V87c]
  
  dataset[ , V88                      :=mpasivos_margen/Visa_msaldopesos]
  
  dataset[ , V88b                      :=1/V88]
  
  dataset[ , V89                      :=mpasivos_margen/mrentabilidad_annual]
  
  dataset[ , V89b                      :=1/V89]
  
  dataset[ , V90                      :=mpasivos_margen/mcomisiones_mantenimiento]
  
  dataset[ , V90b                      :=1/V90]
  
  dataset[ , V91                      :=mpasivos_margen/mrentabilidad]
  
  dataset[ , V91b                      :=1/V91]
  
  dataset[ , V92                      :=mpasivos_margen/mcomisiones_otras]
  
  dataset[ , V92b                      :=1/V92]
  
  dataset[ , V93                      :=mpasivos_margen/mcomisiones]
  
  dataset[ , V93b                      :=1/V93]
  
  dataset[ , V94                      :=mpasivos_margen/mautoservicio]
  
  dataset[ , V94b                      :=1/V94]
  
  dataset[ , V94c                      :=mpasivos_margen/Visa_mpagominimo]
  
  dataset[ , V94d                      :=1/V94c]
  
  dataset[ , V95                      :=Visa_msaldopesos/mrentabilidad_annual]
  
  dataset[ , V95b                      :=1/V95]
  
  dataset[ , V96                      :=Visa_msaldopesos/mcomisiones_mantenimiento]
  
  dataset[ , V96b                      :=1/V96]
  
  dataset[ , V97                      :=Visa_msaldopesos/mrentabilidad]
  
  dataset[ , V97b                      :=1/V97]
  
  dataset[ , V98                      :=Visa_msaldopesos/mcomisiones_otras]
  
  dataset[ , V98b                      :=1/V98]
  
  dataset[ , V99                      :=Visa_msaldopesos/mcomisiones]
  
  dataset[ , V99b                      :=1/V99]
  
  dataset[ , V100                      :=Visa_msaldopesos/mautoservicio]
  
  dataset[ , V100b                      :=1/V100]
  
  dataset[ , V100c                      :=Visa_msaldopesos/Visa_mpagominimo]
  
  dataset[ , V100d                      :=1/V100c]
  
  dataset[ , V101                      :=mrentabilidad_annual/mcomisiones_mantenimiento]
  
  dataset[ , V101b                      :=1/V101]
  
  dataset[ , V102                      :=mrentabilidad_annual/mrentabilidad]
  
  dataset[ , V102b                      :=1/V102]
  
  dataset[ , V103                      :=mrentabilidad_annual/mcomisiones_otras]
  
  dataset[ , V103b                      :=1/V103]
  
  dataset[ , V104                      :=mrentabilidad_annual/mcomisiones]
  
  dataset[ , V104b                      :=1/V104]
  
  dataset[ , V105                      :=mrentabilidad_annual/mautoservicio]
  
  dataset[ , V105b                      :=1/V105]
  
  dataset[ , V105c                      :=mrentabilidad_annual/Visa_mpagominimo]
  
  dataset[ , V105d                      :=1/V105c]
  
  dataset[ , V106                      :=mcomisiones_mantenimiento/mrentabilidad]
  
  dataset[ , V106b                      :=1/V106]
  
  dataset[ , V107                      :=mcomisiones_mantenimiento/mcomisiones_otras]
  
  dataset[ , V107b                      :=1/V107]
  
  dataset[ , V108                      :=mcomisiones_mantenimiento/mcomisiones]
  
  dataset[ , V108b                      :=1/V108]
  
  dataset[ , V110                      :=mcomisiones_mantenimiento/mautoservicio]
  
  dataset[ , V109b                      :=1/V110]
  
  dataset[ , V110b                      :=mcomisiones_mantenimiento/Visa_mpagominimo]
  
  dataset[ , V110c                      :=1/V110b]
  
  dataset[ , V111                      :=mrentabilidad/mcomisiones_otras]
  
  dataset[ , V111b                      :=1/V111]
  
  dataset[ , V112                      :=mrentabilidad/mcomisiones]
  
  dataset[ , V112b                      :=1/V112]
  
  dataset[ , V113                      :=mrentabilidad/mautoservicio]
  
  dataset[ , V113b                      :=1/V113]
  
  dataset[ , V114                      :=mcomisiones_otras/mcomisiones]
  
  dataset[ , V114b                      :=1/V114]
  
  dataset[ , V115                      :=mcomisiones_otras/mautoservicio]
  
  dataset[ , V115b                      :=1/V115]
  
  dataset[ , V115c                      :=mcomisiones_otras/Visa_mpagominimo]
  
  dataset[ , V115d                      :=1/V115c]
  
  dataset[ , V116                      :=mcomisiones/mautoservicio]
  
  dataset[ , V116b                      :=1/V116]
  
  dataset[ , V116c                      :=mcomisiones/Visa_mpagominimo]
  
  dataset[ , V116d                      :=1/V116c]
  
  dataset[ , V117                      :=mautoservicio/Visa_mpagominimo]
  
  dataset[ , V117b                      :=1/V117]
  
  dataset[ , V118                      :=Visa_status/cpayroll_trx]
  
  dataset[ , V118b                      :=1/V118]
  
  dataset[ , V119                      :=Visa_status/(cpayroll_trx+ctarjeta_visa_transacciones+ccomisiones_otras+ccomisiones_mantenimiento+ctarjeta_debito_transacciones)]
  
  #CREO NUEVAS VARIABLES CON LAS SURGEN DEL MODELO E1026_135
  
  dataset[ , V120                      :=ctrx_quarter/V16]
  
  dataset[ , V120i                      :=1/V120]
  
  dataset[ , V121                      :=ctrx_quarter/V30]
  
  dataset[ , V121i                      :=1/V121]
  
  dataset[ , V122                      :=ctrx_quarter/V44b]
  
  dataset[ , V122i                      :=1/V122]
  
  dataset[ , V123                      :=ctrx_quarter/V48]
  
  dataset[ , V123i                      :=1/V123]
  
  dataset[ , V124                      :=ctrx_quarter/V40]
  
  dataset[ , V124i                      :=1/V124]
  
  dataset[ , V125                      :=ctrx_quarter/cpayroll_trx]
  
  dataset[ , V125i                      :=1/V125]
  
  dataset[ , V126                      :=ctrx_quarter/V90b]
  
  dataset[ , V126i                      :=1/V126]
  
  dataset[ , V127                      :=ctrx_quarter/mcuentas_saldo]
  
  dataset[ , V127i                      :=1/V127]
  
  dataset[ , V128                      :=ctrx_quarter/ctarjeta_visa_transacciones]
  
  dataset[ , V128i                      :=1/V128]
  
  dataset[ , V129                      :=V16/V30]
  
  dataset[ , V129i                      :=1/V129]
  
  dataset[ , V130                      :=V16/V44b]
  
  dataset[ , V130i                      :=1/V130]
  
  dataset[ , V131                      :=V16/V48]
  
  dataset[ , V131i                      :=1/V131]
  
  dataset[ , V132                      :=V16/V40]
  
  dataset[ , V132i                      :=1/V132]
  
  dataset[ , V133                      :=V16/cpayroll_trx]
  
  dataset[ , V133i                      :=1/V133]
  
  dataset[ , V134                      :=V16/V90b]
  
  dataset[ , V134i                      :=1/V134]
  
  dataset[ , V135                      :=V16/mcuentas_saldo]
  
  dataset[ , V135i                      :=1/V135]
  
  dataset[ , V136                      :=V16/ctarjeta_visa_transacciones]
  
  dataset[ , V136i                      :=1/V136]
  
  dataset[ , V137                      :=V30/V44b]
  
  dataset[ , V137i                      :=1/V137]
  
  dataset[ , V138                      :=V30/V48]
  
  dataset[ , V138i                      :=1/V138]
  
  dataset[ , V139                      :=V30/V40]
  
  dataset[ , V139i                      :=1/V139]
  
  dataset[ , V140                      :=V30/cpayroll_trx]
  
  dataset[ , V140i                      :=1/V140]
  
  dataset[ , V141                      :=V30/V90b]
  
  dataset[ , V141i                      :=1/V141]
  
  dataset[ , V142                      :=V30/mcuentas_saldo]
  
  dataset[ , V142i                      :=1/V142]
  
  dataset[ , V143                      :=V30/ctarjeta_visa_transacciones]
  
  dataset[ , V143i                      :=1/V143]  
  
  dataset[ , V144                      :=V44b/V48]
  
  dataset[ , V144i                      :=1/V144]
  
  dataset[ , V145                      :=V44b/V40]
  
  dataset[ , V145i                      :=1/V145]
  
  dataset[ , V146                      :=V44b/cpayroll_trx]
  
  dataset[ , V146i                      :=1/V146]
  
  dataset[ , V147                      :=V44b/V90b]
  
  dataset[ , V147i                      :=1/V147]
  
  dataset[ , V148                      :=V44b/mcuentas_saldo]
  
  dataset[ , V148i                      :=1/V148]
  
  dataset[ , V149                      :=V44b/ctarjeta_visa_transacciones]
  
  dataset[ , V149i                      :=1/V149]
  
  dataset[ , V150                      :=V48/V40]
  
  dataset[ , V150i                      :=1/V150]
  
  dataset[ , V151                      :=V48/cpayroll_trx]
  
  dataset[ , V151i                      :=1/V151]
  
  dataset[ , V152                      :=V48/V90b]
  
  dataset[ , V152i                      :=1/V152]
  
  dataset[ , V153                      :=V48/mcuentas_saldo]
  
  dataset[ , V153i                      :=1/V153]
  
  dataset[ , V154                      :=V48/ctarjeta_visa_transacciones]
  
  dataset[ , V154i                      :=1/V154]
  
  dataset[ , V155                      :=cpayroll_trx/V90b]
  
  dataset[ , V155i                      :=1/V155]
  
  dataset[ , V156                      :=cpayroll_trx/mcuentas_saldo]
  
  dataset[ , V156i                      :=1/V156]
  
  dataset[ , V157                      :=cpayroll_trx/ctarjeta_visa_transacciones]
  
  dataset[ , V157i                      :=1/V157]
  
  dataset[ , V158                      :=V90b/mcuentas_saldo]
  
  dataset[ , V158i                      :=1/V158]
  
  dataset[ , V159                      :=V90b/ctarjeta_visa_transacciones]
  
  dataset[ , V159i                      :=1/V159]
  
  dataset[ , V160                      :=mcuentas_saldo/ctarjeta_visa_transacciones]
  
  dataset[ , V160i                      :=1/V160]
  
  #CREO NUEVAS VARIABLES CON LAS SURGEN DEL MODELO E1028_150
  
  dataset[ , V161                      :=ctrx_quarter/V135]
  
  dataset[ , V161i                      :=1/V161]
  
  dataset[ , V162                      :=ctrx_quarter/V138]
  
  dataset[ , V162i                      :=1/V162]
  
  dataset[ , V163                      :=ctrx_quarter/mactivos_margen]
  
  dataset[ , V163i                      :=1/V163]
  
  dataset[ , V164                      :=ctrx_quarter/V6]
  
  dataset[ , V164i                      :=1/V164]
  
  dataset[ , V165                      :=ctrx_quarter/V131]
  
  dataset[ , V165i                      :=1/V165]
  
  dataset[ , V166                      :=V16/V138]
  
  dataset[ , V166i                      :=1/V166]
  
  dataset[ , V167                      :=V16/V48]
  
  dataset[ , V167i                      :=1/V167]
  
  dataset[ , V168                      :=V16/V138i]
  
  dataset[ , V168i                      :=1/V168]
  
  dataset[ , V169                      :=V16/V6]
  
  dataset[ , V169i                      :=1/V169]
  
  dataset[ , V170                      :=V135/V138]
  
  dataset[ , V170i                      :=1/V170]
  
  dataset[ , V171                      :=V135/V48]
  
  dataset[ , V171i                      :=1/V171]
  
  dataset[ , V172                      :=V135/mcuentas_saldo]
  
  dataset[ , V172i                      :=1/V172]
  
  dataset[ , V173                      :=V135/mactivos_margen]
  
  dataset[ , V173i                      :=1/V173]
  
  dataset[ , V174                      :=V135/V6]
  
  dataset[ , V174i                      :=1/V174]
  
  dataset[ , V175                      :=V135/V131]
  
  dataset[ , V175i                      :=1/V175]
  
  dataset[ , V176                      :=V138/V48]
  
  dataset[ , V176i                      :=1/V176]
  
  dataset[ , V177                      :=V138/mcuentas_saldo]
  
  dataset[ , V177i                      :=1/V177]
  
  dataset[ , V178                      :=V138/mactivos_margen]
  
  dataset[ , V178i                      :=1/V178]
  
  dataset[ , V179                      :=V138/V6]
  
  dataset[ , V179i                      :=1/V179]
  
  dataset[ , V180                      :=V138/V131]
  
  dataset[ , V180i                      :=1/V180]
  
  dataset[ , V181                      :=V6/V131]
  
  dataset[ , V181i                      :=1/V181]
  
  #CREO NUEVAS VARIABLES CON LAS SURGEN DEL MODELO E1107
  
  dataset[ , V182                      :=mcuentas_saldo/mactivos_margen]
  
  dataset[ , V182i                      :=1/V182]
  
  dataset[ , V183                      :=mcuentas_saldo/mvr_msaldototal]
  
  dataset[ , V183i                      :=1/V183]
  
  dataset[ , V184                      :=mcuentas_saldo/mv_status01]
  
  dataset[ , V184i                      :=1/V184]
  
  dataset[ , V185                      :=mcuentas_saldo/mprestamos_personales]
  
  dataset[ , V185i                      :=1/V185]
  
  dataset[ , V186                      :=mactivos_margen/mvr_msaldototal]
  
  dataset[ , V186i                      :=1/V186]
  
  dataset[ , V187                      :=mactivos_margen/mv_status01]
  
  dataset[ , V187i                      :=1/V187]
  
  dataset[ , V188                      :=mactivos_margen/mprestamos_personales]
  
  dataset[ , V188i                      :=1/V188]
  
  
  
  
  
  
  
  
  
  
  
  
  
  #valvula de seguridad para evitar valores infinitos
  #paso los infinitos a NULOS
  infinitos      <- lapply(names(dataset),function(.name) dataset[ , sum(is.infinite(get(.name)))])
  infinitos_qty  <- sum( unlist( infinitos) )
  if( infinitos_qty > 0 )
  {
    cat( "ATENCION, hay", infinitos_qty, "valores infinitos en tu dataset. Seran pasados a NA\n" )
    dataset[mapply(is.infinite, dataset)] <- NA
  }


  #valvula de seguridad para evitar valores NaN  que es 0/0
  #paso los NaN a 0 , decision polemica si las hay
  #se invita a asignar un valor razonable segun la semantica del campo creado
  nans      <- lapply(names(dataset),function(.name) dataset[ , sum(is.nan(get(.name)))])
  nans_qty  <- sum( unlist( nans) )
  if( nans_qty > 0 )
  {
    cat( "ATENCION, hay", nans_qty, "valores NaN 0/0 en tu dataset. Seran pasados arbitrariamente a 0\n" )
    cat( "Si no te gusta la decision, modifica a gusto el programa!\n\n")
    dataset[mapply(is.nan, dataset)] <- 0
  }

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#esta funcion supone que dataset esta ordenado por   <numero_de_cliente, foto_mes>
#calcula el lag y el delta lag

Lags  <- function( dataset, cols, nlag, deltas )
{

  sufijo  <- paste0( "_lag", nlag )

  dataset[ , paste0( cols, sufijo) := shift(.SD, nlag, NA, "lag"), 
             by= numero_de_cliente, 
             .SDcols= cols]

  #agrego los deltas de los lags, con un "for" nada elegante
  if( deltas )
  {
    sufijodelta  <- paste0( "_delta", nlag )

    for( vcol in cols )
    {
     dataset[,  paste0(vcol, sufijodelta) := get( vcol)  - get(paste0( vcol, sufijo))]
    }
  }

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#calcula el promedio de los ultimos  nhistoria meses

Promedios  <- function( dataset, cols, nhistoria )
{

  sufijo  <- paste0( "_avg", nhistoria )
  
  dataset[ , paste0( cols, sufijo) := frollmean(x=.SD, n=nhistoria, na.rm=TRUE, algo="fast", align="right"), 
             by= numero_de_cliente, 
             .SDcols= cols]

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#calcula el minimo de los ultimos  nhistoria meses

Minimos  <- function( dataset, cols, nhistoria )
{

  sufijo  <- paste0( "_min", nhistoria )

  dataset[ , paste0( cols, sufijo) := frollapply(x=.SD, FUN="min", n=nhistoria, align="right"), 
             by= numero_de_cliente, 
             .SDcols= cols]

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------
#calcula el maximo de los ultimos  nhistoria meses

Maximos  <- function( dataset, cols, nhistoria )
{

  sufijo  <- paste0( "_max", nhistoria )

  dataset[ , paste0( cols, sufijo) := frollapply(x=.SD, FUN="max", n=nhistoria, na.rm=TRUE, align="right"), 
             by= numero_de_cliente, 
             .SDcols= cols]

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------

#se calculan para los 6 meses previos el minimo, maximo y tendencia calculada con cuadrados minimos
#la formual de calculo de la tendencia puede verse en https://stats.libretexts.org/Bookshelves/Introductory_Statistics/Book%3A_Introductory_Statistics_(Shafer_and_Zhang)/10%3A_Correlation_and_Regression/10.04%3A_The_Least_Squares_Regression_Line
#para la maxíma velocidad esta funcion esta escrita en lenguaje C, y no en la porqueria de R o Python

Rcpp::cppFunction('NumericVector fhistC(NumericVector pcolumna, IntegerVector pdesde ) 
{
  // [[Rcpp::plugins(openmp)]]
  /* Aqui se cargan los valores para la regresion */
  double  x[100] ;
  double  y[100] ;

  int n = pcolumna.size();
  NumericVector out( n );


  //#if defined(_OPENMP)
  //#pragma omp parallel for
  //#endif
  for(int i = 0; i < n; i++)
  {
    int  libre    = 0 ;
    int  xvalor   = 1 ;

    for( int j= pdesde[i]-1;  j<=i; j++ )
    {
       double a = pcolumna[j] ;

       if( !R_IsNA( a ) ) 
       {
          y[ libre ]= a ;
          x[ libre ]= xvalor ;
          libre++ ;
       }

       xvalor++ ;
    }

    /* Si hay al menos dos valores */
    if( libre > 1 )
    {
      double  xsum  = x[0] ;
      double  ysum  = y[0] ;
      double  xysum = xsum * ysum ;
      double  xxsum = xsum * xsum ;
      double  vmin  = y[0] ;
      double  vmax  = y[0] ;

      for( int h=1; h<libre; h++)
      { 
        xsum  += x[h] ;
        ysum  += y[h] ; 
        xysum += x[h]*y[h] ;
        xxsum += x[h]*x[h] ;

        if( y[h] < vmin )  vmin = y[h] ;
        if( y[h] > vmax )  vmax = y[h] ;
      }

      out[ i ]  =  (libre*xysum - xsum*ysum)/(libre*xxsum -xsum*xsum) ;
    }
    else
    {
      out[ i ]  =  NA_REAL ; 
    }
  }

  return  out;
}')

#------------------------------------------------------------------------------
#calcula la tendencia de las variables cols de los ultimos 6 meses
#la tendencia es la pendiente de la recta que ajusta por cuadrados minimos

Tendencia  <- function( dataset, cols )
{
  #Esta es la cantidad de meses que utilizo para la historia
  ventana_regresion  <- 6

  last  <- nrow( dataset )

  #creo el vector_desde que indica cada ventana
  #de esta forma se acelera el procesamiento ya que lo hago una sola vez
  vector_ids   <- dataset$numero_de_cliente

  vector_desde  <- seq( -ventana_regresion+2,  nrow(dataset)-ventana_regresion+1 )
  vector_desde[ 1:ventana_regresion ]  <-  1

  for( i in 2:last )  if( vector_ids[ i-1 ] !=  vector_ids[ i ] ) {  vector_desde[i] <-  i }
  for( i in 2:last )  if( vector_desde[i] < vector_desde[i-1] )  {  vector_desde[i] <-  vector_desde[i-1] }

  for(  campo  in   cols )
  {
    nueva_col     <- fhistC( dataset[ , get(campo) ], vector_desde ) 

    dataset[ , paste0( campo, "_tend") := nueva_col[ (0*last +1):(1*last) ]  ]
  }

}
#------------------------------------------------------------------------------
VPOS_CORTE  <- c()

fganancia_lgbm_meseta  <- function(probs, datos) 
{
  vlabels  <- getinfo(datos, "label")
  vpesos   <- getinfo(datos, "weight")

  #solo sumo 48750 si vpesos > 1, hackeo 
  tbl  <- as.data.table( list( "prob"=probs, "gan"= ifelse( vlabels==1 & vpesos > 1, 48750, -1250 ) ) )

  setorder( tbl, -prob )
  tbl[ , posicion := .I ]
  tbl[ , gan_acum :=  cumsum( gan ) ]
  setorder( tbl, -gan_acum )   #voy por la meseta

  gan  <- mean( tbl[ 1:500,  gan_acum] )  #meseta de tamaño 500

  pos_meseta  <- tbl[ 1:500,  median(posicion)]
  VPOS_CORTE  <<- c( VPOS_CORTE, pos_meseta )

  return( list( "name"= "ganancia", 
                "value"=  gan,
                "higher_better"= TRUE ) )
}
#------------------------------------------------------------------------------
#Elimina del dataset las variables que estan por debajo de la capa geologica de canaritos

CanaritosImportancia  <- function( dataset )
{

  gc()
  dataset[ , clase01:= ifelse( clase_ternaria=="CONTINUA", 0, 1 ) ]

  for( i  in 1:(ncol(dataset)/5))  dataset[ , paste0("canarito", i ) :=  runif( nrow(dataset))]

  campos_buenos  <- setdiff( colnames(dataset), c("clase_ternaria","clase01" ) )

  azar  <- runif( nrow(dataset) )
  entrenamiento  <-  dataset[ , foto_mes>= 202001 &  foto_mes<= 202010 &  foto_mes!=202006 & ( clase01==1 | azar < 0.10 ) ]

  dtrain  <- lgb.Dataset( data=    data.matrix(  dataset[ entrenamiento==TRUE, campos_buenos, with=FALSE]),
                          label=   dataset[ entrenamiento==TRUE, clase01],
                          weight=  dataset[ entrenamiento==TRUE, ifelse(clase_ternaria=="BAJA+2", 1.0000001, 1.0)] )

  dvalid  <- lgb.Dataset( data=    data.matrix(  dataset[ foto_mes==202011, campos_buenos, with=FALSE]),
                          label=   dataset[ foto_mes==202011, clase01],
                          weight=  dataset[ foto_mes==202011, ifelse(clase_ternaria=="BAJA+2", 1.0000001, 1.0)] )


  param <- list( objective= "binary",
                 metric= "custom",
                 first_metric_only= TRUE,
                 boost_from_average= TRUE,
                 feature_pre_filter= FALSE,
                 verbosity= -100,
                 seed= 199,              #Puse el mío
                 max_depth=  -1,         # -1 significa no limitar,  por ahora lo dejo fijo
                 min_gain_to_split= 0.0, #por ahora, lo dejo fijo
                 lambda_l1= 0.0,         #por ahora, lo dejo fijo
                 lambda_l2= 0.0,         #por ahora, lo dejo fijo
                 max_bin= 31,            #por ahora, lo dejo fijo
                 num_iterations= 9999,   #un numero muy grande, lo limita early_stopping_rounds
                 force_row_wise= TRUE,    #para que los alumnos no se atemoricen con tantos warning
                 learning_rate= 0.02, 
                 feature_fraction= 0.50,
                 min_data_in_leaf= 4000,
                 num_leaves= 600,
                 early_stopping_rounds= 200 )

  modelo  <- lgb.train( data= dtrain,
                        valids= list( valid= dvalid ),
                        eval= fganancia_lgbm_meseta,
                        param= param,
                        verbose= -100 )

  tb_importancia  <- lgb.importance( model= modelo )
  tb_importancia[  , pos := .I ]
  
  fwrite( tb_importancia, file="./work/impo.txt", sep="\t" )
  
  umbral  <- tb_importancia[ Feature %like% "canarito", median(pos) - sd(pos) ]
  col_inutiles  <- tb_importancia[ pos >= umbral | Feature %like% "canarito",  Feature ]

  for( col in col_inutiles )
  {
    dataset[  ,  paste0(col) := NULL ]
  }

  rm( dtrain, dvalid )
  gc()

  ReportarCampos( dataset )
}
#------------------------------------------------------------------------------

correr_todo  <- function( palancas )
{
  #cargo el dataset ORIGINAL
  dataset1  <- fread( "./datasetsOri/paquete_premium_202009.csv")
  dataset2  <- fread( "./datasetsOri/paquete_premium_202011.csv")

  dataset   <- rbind( dataset1, dataset2 )
  rm( dataset1, dataset2 )
  gc()

  setorder(  dataset, numero_de_cliente, foto_mes )  #ordeno el dataset

  AgregarMes( dataset )  #agrego el mes del año

  if( length(palancas$variablesdrift) > 0 )   DriftEliminar( dataset, palancas$variablesdrift )

  if( palancas$dummiesNA )  DummiesNA( dataset )  #esta linea debe ir ANTES de Corregir  !!

  if( palancas$corregir )  Corregir( dataset )  #esta linea debe ir DESPUES de  DummiesNA

  if( palancas$nuevasvars )  AgregarVariables( dataset )

  cols_analiticas  <- setdiff( colnames(dataset),  c("numero_de_cliente","foto_mes","mes","clase_ternaria") )

  if( palancas$lag1 )   Lags( dataset, cols_analiticas, 1, palancas$delta1 )
  if( palancas$lag2 )   Lags( dataset, cols_analiticas, 2, palancas$delta2 )
  if( palancas$lag3 )   Lags( dataset, cols_analiticas, 3, palancas$delta3 )
  if( palancas$lag4 )   Lags( dataset, cols_analiticas, 4, palancas$delta4 )
  if( palancas$lag5 )   Lags( dataset, cols_analiticas, 5, palancas$delta5 )
  if( palancas$lag6 )   Lags( dataset, cols_analiticas, 6, palancas$delta6 )

  if( palancas$promedio3 )  Promedios( dataset, cols_analiticas, 3 )
  if( palancas$promedio6 )  Promedios( dataset, cols_analiticas, 6 )

  if( palancas$minimo3 )  Minimos( dataset, cols_analiticas, 3 )
  if( palancas$minimo6 )  Minimos( dataset, cols_analiticas, 6 )

  if( palancas$maximo3 )  Maximos( dataset, cols_analiticas, 3 )
  if( palancas$maximo6 )  Maximos( dataset, cols_analiticas, 6 )

  if( palancas$tendencia6 )  Tendencia( dataset, cols_analiticas)


  if( palancas$canaritosimportancia )  CanaritosImportancia( dataset )



  #dejo la clase como ultimo campo
  nuevo_orden  <- c( setdiff( colnames( dataset ) , "clase_ternaria" ) , "clase_ternaria" )
  setcolorder( dataset, nuevo_orden )

  #Grabo el dataset
  fwrite( dataset,
          paste0( "./datasets/dataset_epic_simple_", palancas$version, ".csv.gz" ),
          logical01 = TRUE,
          sep= "," )

}
#------------------------------------------------------------------------------

#Aqui empieza el programa


correr_todo( palancas )


quit( save="no" )


