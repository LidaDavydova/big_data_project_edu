# Columns Description

## Columns from original dataset (table staging_flights)

id_basica: Numeric code identifier of the basic step in the system

sg_empresa_icao: ICAO acronym of the airline. It refers to the designator of the air transport company obtained from the ICAO (International Civil Aviation Organization).

dt_partida_real: Date of realization. It refers to the full take-off date of the stage, calculated by the criterion of the shoe and barefoot.

cd_di: Identifier Digit. It refers to the character used to identify the type of authorization for each flight stage, assuming the following values:
a) 0 (zero) - Regular Step
b) 2 (two) - Extra Step
C) 3 (three) - Return Stage
d) 4 (four) - Step Inclusion
e) 6 (six) - Unpaid Step Without Object Transport
f) 7 (sen) - Charter Flight Stage
g) 9(nine) - Charter Flight Stage
h) D - Duplicate Flight Stage
i) E - Unpaid Stage With Object Transport

ds_grupo_di: Description of the DI group or Flight Type. Refers to the grouping of DI classifications into types of operations:
Regular (DIs 0, 4 or C)
Non-regular (DIs 1,2,5,7,8,9 or D)
Unproductive (Toother DIs)

cd_tipo_linha: Character that identifies the type of line. The Line Type identifies the type of operation performed on the flight, and can be classified into:
a) N - Mixed Domestic: for passenger or mixed air transport operations, in which all aerodromes involved are located simultaneously in Brazilian territory;
b) C - Domestic Cargueira: for exclusive air transport operations of cargo and/or postal bag in which all the aerodromes involved are located simultaneously in Brazilian territory;
c) I - Mixed International: for passenger or mixed air transport operations, in which at least one of the aerodromes involved is located in foreign territory;
d) G - International Cargueira: for exclusive air transport operations of cargo and/or postal network in which at least one of the aerodromes involved is located in foreign territory;

sg_icao_origem: ICAO acronym of the aerodrome of origin.

nm_municipio_origem: Name of the municipality of origin.

sg_uf_origem: Acronym of the Federative Unit of origin.

nm_pais_origem: Name of the country of origin.

nm_continente_origem: Name of the continent of origin.

sg_icao_destino: ICAO acronym of the destination airfield.

nm_municipio_destino: Name of the destination municipality.

sg_uf_destino: Acronym of the destination Federative Unit.

nm_pais_destino: Name of the destination country.

nm_continente_destino: Name of the destination continent.

km_distancia: Distance, expressed in kilometers, between the aerodromes of origin and destination of the stage, considering the curvature of planet earth.

nr_passag_pagos: Number of passengers who occupy seats 

nr_passag_gratis: Number of passengers who occupy seats marketed to the public, but do not generate revenue, with the purchase of seats, for the air transport company.

nr_rpk: Refers to the volume of Passenger Kilometers Transported (Revenue Passenger Kilometer). It is calculated, in each basic step, by the product between the number of passengers paid and the distance of the stage.

nr_ask: Refers to the volume of Seat Kilometers Offered (Available Seat Kilometer). It is calculated, in each basic step, by the product between the number of seats offered and the distance of the stage.

nr_carga_paga_km: Refers to the product between the amount of load paid (kg) and the distance of the basic step (km).

nr_carga_gratis_km: Refers to the product between the amount of free load (kg) and the distance of the basic step (km).


