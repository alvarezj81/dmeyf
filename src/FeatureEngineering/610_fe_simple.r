#Feature Engineering
#creo nuevas variables dentro del mismo mes
#Condimentar a gusto con nuevas variables

#limpio la memoria
rm( list=ls() )
gc()

require("data.table")



#Establezco el Working Directory
setwd( "/media/juan/Extraible/Maestria/EyF" )


EnriquecerDataset <- function( dataset , arch_destino )
{
  columnas_originales <-  copy(colnames( dataset ))

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
  dataset[ , mv_mpagominimo          := rowSums( cbind( Master_mpagominimo,  Visa_mpagominimo) , na.rm=TRUE ) ]

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
  dataset[ , mvr_mpagominimo         := mv_mpagominimo  / mv_mlimitecompra ]

  #VARIABLES QUE INTENTO CREAR YO
  
  #CONSUMO TARJETAS / SALDO PF
  
  dataset[ , V1                      :=(Master_mconsumototal+Visa_mconsumototal)/mplazo_fijo_pesos]
  
  #CONSUMO TARJETAS / SALDO en Cuentas convertido todo a $
  
  dataset[ , V2                      :=(Master_mconsumototal+Visa_mconsumototal)/mcuentas_saldo]
  
  # PAGO SUELDO / LIMITE TARJETA. Si cobro mucho puede que sea indemnización y tal vez luego de cobrar eso se vaya del banco
  #V3 con Master
  dataset[ , V3                       :=mpayroll/Master_mlimitecompra]
  #V4 con Visa
  dataset[ , V4                       :=mpayroll/Visa_mlimitecompra]
  
  #EDAD con Saldo caja de Ahorro
  
  dataset[ , V5                       := mcaja_ahorro/cliente_edad]
  
  #Antiguedad cliente con Saldo
  
  dataset[ , V6                        :=mcaja_ahorro/(cliente_antiguedad/12)]

  #USO AHORA ALGUNAS DE LAS QUE OBTUVE COMO MÁS IMPORTANTES
  
  dataset[ , V7                        :=(mpayroll+mpayroll2)/cpayroll_trx]
  
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

  #FIN de la seccion donde se deben hacer cambios con variables nuevas

  columnas_extendidas <-  copy( setdiff(  colnames(dataset), columnas_originales ) )

  #grabo con nombre extendido
  fwrite( dataset,
          file=arch_destino,
          sep= "," )
}
#------------------------------------------------------------------------------

dir.create( "./datasets/" )


#lectura rapida del dataset  usando fread  de la libreria  data.table
dataset1  <- fread("./datasetsOri/paquete_premium_202009.csv")
dataset2  <- fread("./datasetsOri/paquete_premium_202011.csv")

EnriquecerDataset( dataset1, "./datasets/paquete_premium_202009_ext.csv" )
EnriquecerDataset( dataset2, "./datasets/paquete_premium_202011_ext.csv" )

quit( save="no")

