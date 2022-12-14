# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook to do complete history load for data source ngca

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop table if it exists

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists cummins.ngca

# COMMAND ----------

ngca_engine_list =[
    '33198986A'
    ,'33195879RHE'
    ,'33194148RH'
    ,'33193809RHE'
    ,'33193781LHE'
    ,'33193196LH'
    ,'33190611LHE'
    ,'33190595RHE'
    ,'33189954LHE'
    ,'33185630A'
    ,'33181947A'
    ,'33173562A'
    ,'33172087LHE'
    ,'33172078LHE'
    ,'33159158B'
    ,'33154062C'
    ,'33151595C'
    ,'33150576C'
    ,'33143154D'
    ,'223345Test'
    ,'223344Test'
    ,'1010001A'
    ,'1010000A'
    ,'99999999'
    ,'98909876'
    ,'98000486'
    ,'98000419'
    ,'98000408'
    ,'98000390'
    ,'98000388'
    ,'98000363'
    ,'98000342'
    ,'98000327'
    ,'98000274'
    ,'98000273'
    ,'98000254'
    ,'98000247'
    ,'98000216'
    ,'92093000'
    ,'91197300'
    ,'90151000'
    ,'89932246'
    ,'89911175'
    ,'89889800'
    ,'89871855'
    ,'89765936'
    ,'89684084'
    ,'89529499'
    ,'89452099'
    ,'89285378'
    ,'89078909'
    ,'88888899'
    ,'88888888'
    ,'85004245'
    ,'85002152'
    ,'84387520'
    ,'82505028'
    ,'82505027'
    ,'82505019'
    ,'82505018'
    ,'82505015'
    ,'82505014'
    ,'80219856'
    ,'80205890'
    ,'80103664'
    ,'80098544'
    ,'80093248'
    ,'80071842'
    ,'80000254'
    ,'80000253'
    ,'80000252'
    ,'80000251'
    ,'79974945'
    ,'79973832'
    ,'79958979'
    ,'79846285'
    ,'79748776'
    ,'79732932'
    ,'79700234'
    ,'79673864'
    ,'79673862'
    ,'79673861'
    ,'79646464'
    ,'79630238'
    ,'79596473'
    ,'79526020'
    ,'79507423'
    ,'79313160'
    ,'79268279'
    ,'79007181'
    ,'78787878'
    ,'78522507'
    ,'78023685'
    ,'78008693'
    ,'77777779'
    ,'77777778'
    ,'77777777'
    ,'77777772'
    ,'77777771'
    ,'76666671'
    ,'76666670'
    ,'76380923'
    ,'76330010'
    ,'75057569'
    ,'74698001'
    ,'74697998'
    ,'74697995'
    ,'74431086'
    ,'74406250'
    ,'74232046'
    ,'74225200'
    ,'74000199'
    ,'73838451'
    ,'73751754'
    ,'73613390'
    ,'73512901'
    ,'73443960'
    ,'73347387'
    ,'73347181'
    ,'73343454'
    ,'73327479'
    ,'73326694'
    ,'73159597'
    ,'73110242'
    ,'73092679'
    ,'68614129'
    ,'68347565'
    ,'68332753'
    ,'68311725'
    ,'67890123'
    ,'67777777'
    ,'66876987'
    ,'66666684'
    ,'66666679'
    ,'66666676'
    ,'66666670'
    ,'66666662'
    ,'66554433'
    ,'66306306'
    ,'66306281'
    ,'66306277'
    ,'66306272'
    ,'66305636'
    ,'66305626'
    ,'66305613'
    ,'66305576'
    ,'66305563'
    ,'66305533'
    ,'66305532'
    ,'66305514'
    ,'66305510'
    ,'66305487'
    ,'66305484'
    ,'66305466'
    ,'66305408'
    ,'66305403'
    ,'66305402'
    ,'66305338'
    ,'66305337'
    ,'66305275'
    ,'66305271'
    ,'66305222'
    ,'66305208'
    ,'66305198'
    ,'66305188'
    ,'66305177'
    ,'66305175'
    ,'66305172'
    ,'66305168'
    ,'66305167'
    ,'66305158'
    ,'66305157'
    ,'66305117'
    ,'66305110'
    ,'66305106'
    ,'66305105'
    ,'66305104'
    ,'66305103'
    ,'66305099'
    ,'66305098'
    ,'66305090'
    ,'66305084'
    ,'66305076'
    ,'66305075'
    ,'66305069'
    ,'66305052'
    ,'66305033'
    ,'66305013'
    ,'66305002'
    ,'66305000'
    ,'66304999'
    ,'66304996'
    ,'66304992'
    ,'66304969'
    ,'66304962'
    ,'66304923'
    ,'66304919'
    ,'66304779'
    ,'66304774'
    ,'66304493'
    ,'66304492'
    ,'66304469'
    ,'66304467'
    ,'66304465'
    ,'66304464'
    ,'66304463'
    ,'66304462'
    ,'66304448'
    ,'66304440'
    ,'66304430'
    ,'66304429'
    ,'66304426'
    ,'66304424'
    ,'66304423'
    ,'66304422'
    ,'66304420'
    ,'66304419'
    ,'66304415'
    ,'66304414'
    ,'66304413'
    ,'66304412'
    ,'66304411'
    ,'66304402'
    ,'66304400'
    ,'66304398'
    ,'66304387'
    ,'66304386'
    ,'66304385'
    ,'66304384'
    ,'66304383'
    ,'66304382'
    ,'66304381'
    ,'66304379'
    ,'66304369'
    ,'66304367'
    ,'66304359'
    ,'66304358'
    ,'66304357'
    ,'66304356'
    ,'66304353'
    ,'66304352'
    ,'66304349'
    ,'66304344'
    ,'66304327'
    ,'66304282'
    ,'66304280'
    ,'66304243'
    ,'66304235'
    ,'66304231'
    ,'66304216'
    ,'66304215'
    ,'66304210'
    ,'66304172'
    ,'66304165'
    ,'66304158'
    ,'66304151'
    ,'66304141'
    ,'66304124'
    ,'66304123'
    ,'66304120'
    ,'66304098'
    ,'66304090'
    ,'66304083'
    ,'66304081'
    ,'66304047'
    ,'66304044'
    ,'66304023'
    ,'66304008'
    ,'66303982'
    ,'66303979'
    ,'66303972'
    ,'66303966'
    ,'66303959'
    ,'66303957'
    ,'66303956'
    ,'66303928'
    ,' 66303926'
    ,'66303926'
    ,'66303922'
    ,'66303921'
    ,'66303918'
    ,'66303916'
    ,'66303903'
    ,'66303884'
    ,'66303881'
    ,'66303874'
    ,'66303859'
    ,'66303856'
    ,'66303846'
    ,'66303834'
    ,'66303833'
    ,'66303830'
    ,'66303829'
    ,'66303827'
    ,'66303826'
    ,'66303825'
    ,'66303817'
    ,'66303816'
    ,'66303815'
    ,'66303813'
    ,'66303810'
    ,'66303809'
    ,'66303806'
    ,'66303792'
    ,'66303791'
    ,'66303790'
    ,'66303787'
    ,'66303785'
    ,'66303784'
    ,'66303777'
    ,'66303775'
    ,'66303771'
    ,'66303768'
    ,'66303720'
    ,'66303430'
    ,'66303429'
    ,'66303265'
    ,'66303189'
    ,'66303017'
    ,'66303011'
    ,'66302952'
    ,'66302889'
    ,'66302868'
    ,'66302865'
    ,'66302851'
    ,'66302848'
    ,'66302724'
    ,'66302601'
    ,'66302525'
    ,'66302155'
    ,'66302056'
    ,'66301836'
    ,'66301532'
    ,'64200029'
    ,'64200028'
    ,'64200027'
    ,'64200026'
    ,'64200025'
    ,'64200024'
    ,'64200023'
    ,'64200022'
    ,'64200021'
    ,'64200019'
    ,'64200018'
    ,'64200017'
    ,'64200016'
    ,'64200015'
    ,'63661647'
    ,'63645109'
    ,'63645108'
    ,'60817131'
    ,'60802379'
    ,'60600348'
    ,'60254612'
    ,'58236725'
    ,'57893950'
    ,'56603888'
    ,'56565656'
    ,'55724724'
    ,'55077043'
    ,'51016900'
    ,'46971492'
    ,'46950183'
    ,'46876605'
    ,'46859020'
    ,'46780784'
    ,'46777021'
    ,'46762576'
    ,'46429113'
    ,'46342895'
    ,'46319662'
    ,'46104808'
    ,'39999997'
    ,'37282092'
    ,'37280688'
    ,'37280502'
    ,'37280216'
    ,'37279352'
    ,'37276643'
    ,'37276094'
    ,'37275654'
    ,'37273562'
    ,'37271470'
    ,'37269540'
    ,'37269184'
    ,'37264391'
    ,'37261302'
    ,'37256286'
    ,'37235023'
    ,'37234167'
    ,'37232628'
    ,'37230358'
    ,'37228685'
    ,'37210651'
    ,'37209410'
    ,'37199643'
    ,'37198884'
    ,'37198687'
    ,'37187742'
    ,'37183213'
    ,'37160881'
    ,'36521287'
    ,'36452282'
    ,'36165073'
    ,'36110545'
    ,'35352090'
    ,'35325405'
    ,'35306069'
    ,'35287102'
    ,'35286613'
    ,'35223409'
    ,'35212466'
    ,'35170483'
    ,'35135721'
    ,'35127976'
    ,'34578990'
    ,'34567812'
    ,'33276588'
    ,'33276587'
    ,'33234001'
    ,'33228103'
    ,'33224814'
    ,'33224079'
    ,'33224078'
    ,'33223476'
    ,'33223128'
    ,'33221621'
    ,'33221612'
    ,'33220234'
    ,'33220198'
    ,'33220119'
    ,'33219461'
    ,'33219393'
    ,'33219381'
    ,'33219318'
    ,'33219309'
    ,'33219297'
    ,'33219296'
    ,'33219295'
    ,'33219197'
    ,'33219188'
    ,'33219187'
    ,'33219178'
    ,'33219176'
    ,'33219062'
    ,'33219022'
    ,'33218920'
    ,'33218902'
    ,'33218860'
    ,'33218800'
    ,'33218732'
    ,'33218716'
    ,'33218676'
    ,'33218650'
    ,'33218649'
    ,'33218550'
    ,'33218536'
    ,'33218518'
    ,'33218513'
    ,'33218503'
    ,'33218472'
    ,'33218468'
    ,'33218455'
    ,'33218454'
    ,'33218453'
    ,'33218452'
    ,'33218451'
    ,'33218450'
    ,'33218415'
    ,'33218410'
    ,'33218336'
    ,'33218332'
    ,'33218293'
    ,'33218289'
    ,'33218288'
    ,'33218245'
    ,'33218231'
    ,'33218138'
    ,'33218100'
    ,'33218099'
    ,'33218098'
    ,'33218088'
    ,'33218087'
    ,'33218086'
    ,'33218085'
    ,'33218084'
    ,'33217969'
    ,'33217812'
    ,'33217481'
    ,'33217434'
    ,'33217172'
    ,'33217168'
    ,'33217099'
    ,'33217019'
    ,'33216947'
    ,'33216817'
    ,'33216494'
    ,'33216450'
    ,'33216431'
    ,'33216289'
    ,'33216002'
    ,'33215931'
    ,'33215911'
    ,'33215873'
    ,'33215831'
    ,'33215772'
    ,'33215730'
    ,'33215729'
    ,'33215640'
    ,'33215598'
    ,'33215581'
    ,'33215552'
    ,'33215513'
    ,'33215512'
    ,'33215493'
    ,'33215377'
    ,'33215281'
    ,'33215130'
    ,'33215103'
    ,'33215081'
    ,'33215080'
    ,'33215032'
    ,'33215029'
    ,'33215017'
    ,'33215015'
    ,'33215009'
    ,'33214881'
    ,'33214766'
    ,'33214765'
    ,'33214714'
    ,'33214679'
    ,'33214644'
    ,'33214551'
    ,'33214548'
    ,'33214519'
    ,'33214481'
    ,'33214301'
    ,'33214283'
    ,'33214208'
    ,'33214187'
    ,'33214186'
    ,'33214072'
    ,'33214044'
    ,'33213954'
    ,'33213953'
    ,'33213871'
    ,'33213842'
    ,'33213820'
    ,'33213794'
    ,'33213786'
    ,'33213604'
    ,'33213563'
    ,'33213523'
    ,'33213329'
    ,'33213206'
    ,'33213204'
    ,'33212862'
    ,'33212632'
    ,'33212204'
    ,'33211804'
    ,'33211456'
    ,'33211444'
    ,'33211192'
    ,'33211155'
    ,'33210910'
    ,'33210877'
    ,'33210863'
    ,'33210555'
    ,'33210459'
    ,'33210449'
    ,'33210404'
    ,'33210326'
    ,'33210149'
    ,'33209758'
    ,'33209652'
    ,'33209622'
    ,'33209607'
    ,'33209563'
    ,'33209427'
    ,'33209167'
    ,'33209133'
    ,'33209074'
    ,'33208936'
    ,'33208671'
    ,'33208643'
    ,'33208343'
    ,'33208324'
    ,'33208260'
    ,'33208244'
    ,'33208185'
    ,'33208175'
    ,'33208062'
    ,'33207917'
    ,'33207874'
    ,'33207865'
    ,'33207489'
    ,'33207257'
    ,'33207198'
    ,'33207161'
    ,'33206588'
    ,'33205651'
    ,'33205620'
    ,'33205339'
    ,'33205288'
    ,'33205257'
    ,'33205091'
    ,'33205012'
    ,'33204976'
    ,'33204970'
    ,'33204915'
    ,'33204866'
    ,'33204850'
    ,'33204800'
    ,'33204570'
    ,'33204470'
    ,'33203979'
    ,'33203867'
    ,'33203636'
    ,'33203269'
    ,'33203183'
    ,'33203177'
    ,'33203138'
    ,'33202915'
    ,'33202889'
    ,'33202623'
    ,'33202603'
    ,'33202566'
    ,'33202565'
    ,'33202472'
    ,'33202349'
    ,'33202343'
    ,'33202085'
    ,'33201960'
    ,'33201934'
    ,'33201888'
    ,'33201356'
    ,'33201066'
    ,'33201021'
    ,'33200227'
    ,'33200181'
    ,'33200168'
    ,'33200105'
    ,'33200052'
    ,'33200048'
    ,'33199768'
    ,'33199682'
    ,'33199611'
    ,'33199572'
    ,'33199562'
    ,'33199486'
    ,'33199295'
    ,'33199087'
    ,'33199082'
    ,'33198986'
    ,'33198832'
    ,'33198547'
    ,'33198030'
    ,'33198029'
    ,'33197844'
    ,'33197685'
    ,'33197672'
    ,'33197488'
    ,'33197447'
    ,'33197275'
    ,'33197083'
    ,'33196684'
    ,'33196682'
    ,'33196540'
    ,'33196494'
    ,'33196407'
    ,'33196351'
    ,'33196324'
    ,'33196321'
    ,'33196287'
    ,'33196179'
    ,'33195998'
    ,'33195749'
    ,'33195638'
    ,'33195592'
    ,'33195526'
    ,'33195452'
    ,'33195443'
    ,'33195412'
    ,'33195339'
    ,'33195173'
    ,'33195156'
    ,'33195043'
    ,'33194919'
    ,'33194909'
    ,'33194809'
    ,'33194784'
    ,'33194670'
    ,'33194597'
    ,'33194529'
    ,'33194339'
    ,'33194178'
    ,'33194153'
    ,'33194148'
    ,'33194108'
    ,'33194106'
    ,'33194024'
    ,'33194002'
    ,'33193972'
    ,'33193896'
    ,'33193833'
    ,'33193831'
    ,'33193809'
    ,'33193781'
    ,'33193764'
    ,'33193757'
    ,'33193656'
    ,'33193576'
    ,'33193567'
    ,'33193490'
    ,'33193477'
    ,'33193431'
    ,'33193387'
    ,'33193222'
    ,'33193196'
    ,'33193096'
    ,'33192950'
    ,'33192948'
    ,'33192915'
    ,'33192667'
    ,'33192613'
    ,'33192594'
    ,'33192507'
    ,'33192418'
    ,'33192357'
    ,'33192353'
    ,'33192227'
    ,'33192156'
    ,'33192019'
    ,'33192006'
    ,'33191876'
    ,'33191828'
    ,'33191610'
    ,'33191609'
    ,'33191532'
    ,'33191525'
    ,'33191495'
    ,'33191423'
    ,'33191422'
    ,'33191365'
    ,'33191247'
    ,'33190967'
    ,'33190901'
    ,'33190843'
    ,'33190771'
    ,'33190720'
    ,'33190701'
    ,'33190685'
    ,'33190611'
    ,'33190595'
    ,'33190329'
    ,'33190325'
    ,'33190287'
    ,'33190158'
    ,'33189954'
    ,'33189770'
    ,'33189659'
    ,'33189158'
    ,'33188670'
    ,'33188667'
    ,'33188549'
    ,'33188548'
    ,'33188539'
    ,'33188379'
    ,'33188373'
    ,'33188367'
    ,'33188361'
    ,'33188323'
    ,'33188297'
    ,'33188279'
    ,'33188211'
    ,'33188030'
    ,'33187910'
    ,'33187878'
    ,'33187795'
    ,'33187664'
    ,'33187578'
    ,'33187492'
    ,'33187466'
    ,'33187433'
    ,'33187322'
    ,'33187233'
    ,'33186892'
    ,'33186634'
    ,'33186463'
    ,'33186264'
    ,'33186244'
    ,'33186197'
    ,'33185652'
    ,'33185630'
    ,'33185384'
    ,'33185338'
    ,'33185153'
    ,'33184769'
    ,'33184725'
    ,'33184281'
    ,'33183623'
    ,'33183123'
    ,'33182985'
    ,'33182606'
    ,'33182390'
    ,'33182234'
    ,'33181663'
    ,'33181257'
    ,'33181085'
    ,'33180822'
    ,'33180497'
    ,'33180484'
    ,'33180420'
    ,'33180224'
    ,'33179092'
    ,'33178485'
    ,'33177371'
    ,'33177175'
    ,'33176887'
    ,'33176850'
    ,'33176519'
    ,'33176511'
    ,'33175991'
    ,'33175740'
    ,'33174532'
    ,'33174206'
    ,'33173871'
    ,'33173870'
    ,'33173584'
    ,'33173541'
    ,'33173380'
    ,'33172922'
    ,'33172646'
    ,'33172594'
    ,'33172513'
    ,'33172103'
    ,'33172087'
    ,'33172078'
    ,'33171939'
    ,'33170906'
    ,'33170160'
    ,'33170127'
    ,'33169984'
    ,'33169404'
    ,'33168933'
    ,'33166845'
    ,'33166562'
    ,'33166252'
    ,'33165324'
    ,'33164461'
    ,'33163780'
    ,'33163608'
    ,'33162656'
    ,'33162270'
    ,'33162129'
    ,'33161382'
    ,'33161184'
    ,'33158360'
    ,'33156745'
    ,'33156459'
    ,'33156280'
    ,'33155507'
    ,'33154663'
    ,'33154053'
    ,'33151651'
    ,'33151595'
    ,'33151180'
    ,'33150262'
    ,'33150178'
    ,'33148586'
    ,'33148322'
    ,'33148122'
    ,'33147467'
    ,'33145868'
    ,'33128497'
    ,'30696840'
    ,'30130000'
    ,'30112002'
    ,'30112001'
    ,'30000004'
    ,'30000001'
    ,'29131896'
    ,'29129869'
    ,'29129049'
    ,'29125735'
    ,'29121757'
    ,'29117557'
    ,'28691000'
    ,'28147000 '
    ,'26676549'
    ,'26512026'
    ,'25454358'
    ,'23456712'
    ,'23232390'
    ,'22338492'
    ,'22311478'
    ,'22294761'
    ,'22277389'
    ,'22260304'
    ,'22245588'
    ,'22232113'
    ,'22209381'
    ,'22144184'
    ,'22140446'
    ,'22139068'
    ,'22129421'
    ,'22113249'
    ,'22102020'
    ,'22102002'
    ,'22096825'
    ,'22026759'
    ,'22025200'
    ,'21953916'
    ,'21819211'
    ,'21713302'
    ,'21630539'
    ,'20751000'
    ,'20698000'
    ,'20210401'
    ,'17021994'
    ,'14040174'
    ,'13194148'
    ,'12345678'
    ,'12345615'
    ,'12345614'
    ,'12345613'
    ,'12345612'
    ,'12021160'
    ,'12021158'
    ,'12021157'
    ,'11452629'
    ,'11123456'
    ,'11112020'
    ,'11111111'
    ,'10122020'
    ,'10000001'
    ,'10000000'
    ,'09897899'
    ,'09897865'
    ,'09890987'
    ,'9890987'
    ,'09890981'
    ,'9890981'
    ,'09890976'
    ,'9890976'
    ,'9800025'
    ,'9800024'
    ,'09212020'
    ,'8888109'
    ,'08767999'
    ,'08767998'
    ,'08767997'
    ,'08767891'
    ,'8745678'
    ,'8134648'
    ,'8000360'
    ,'8000254'
    ,'8000247'
    ,'8000035'
    ,'8000034'
    ,'7612345'
    ,'6066244'
    ,'5338888'
    ,'05112020'
    ,'5112020'
    ,'05022022'
    ,'04112020'
    ,'04051020'
    ,'3321873'
    ,'3321698'
    ,'3321680'
    ,'3321511'
    ,'3321509'
    ,'3321401'
    ,'3321382'
    ,'3318626'
    ,'3318082'
    ,'3316998'
    ,'3195200'
    ,'03194855'
    ,'03192095'
    ,'3188845'
    ,'3180822'
    ,'3172594'
    ,'03051020'
    ,'1420006'
    ,'01280929'
    ,'01280920'
    ,'01252021'
    ,'1252021'
    ,'1145262'
    ,'1145247'
    ,'01061020'
    ,'1061020'
    ,'1000000'
    ,'00998877'
    ,'00802201'
    ,'802201'
    ,'00802200'
    ,'802200'
    ,'00802108'
    ,'00802064'
    ,'00802052'
    ,'00802039'
    ,'00802036'
    ,'00802022'
    ,'00800913'
    ,'00800831'
    ,'00800800'
    ,'00800755'
    ,'00800730'
    ,'00800681'
    ,'00800608'
    ,'00800576'
    ,'00800551'
    ,'00800530'
    ,'723723'
    ,'606624'
    ,'510014'
    ,'00509995'
    ,'0502202'
    ,'00324605'
    ,'0090610'
    ,'00060610'
    ,'60610'
    ,'00050610'
    ,'50610'
    ,'00040610'
    ,'40610'
    ,'00020610'
    ,'20610'
    ,'00010610'
    ,'0010610'
    ,'10610'
    ,'00006448'
    ,'6448'
    ,'00006447'
    ,'6447'
    ,'00005631'
    ,'00004764'
    ,'00003512'
    ,'00003361'
    ,'0000023'
    ,'23'
    ,'00000003'
    ,'3'
    ,' 00000000'
    ,'0'
    ,'00000000'
]

# COMMAND ----------

invalid_list = []
valid_list = []

for esn in ngca_engine_list:
    try:
        dbutils.fs.ls(f"s3://fit-all-raw-data-230935021301/{esn}/")
        valid_list.append(esn)
    except Exception as e:
        print(e)
        invalid_list.append(esn)        

# COMMAND ----------

len(valid_list)

# COMMAND ----------

len(invalid_list)

# COMMAND ----------

valid_list

# COMMAND ----------

# MAGIC %md
# MAGIC Load history data for 2021

# COMMAND ----------

from pyspark.sql.functions import *

esn_list = ['66303928']

for esn in esn_list:
  
    try: 
        (
            spark.read.json(f"s3://fit-all-raw-data-230935021301/{esn}/2021-*")
            .select(to_timestamp("frm_gen_ts").alias("frm_gen_ts_modified"), "*")
            .write
            .option("mergeSchema", True)
            .mode('append')
            .format("delta")
            .saveAsTable("cummins.ngca")
        )
        print(f"Completed for - {esn}")
    except Exception as e:
        print(f"Completed for - {esn}")

# COMMAND ----------

# MAGIC %md
# MAGIC Load history data for 2022

# COMMAND ----------

from pyspark.sql.functions import *

esn_list = ['66303928']

for esn in esn_list:
  
    try: 
        (
            spark.read.json(f"s3://fit-all-raw-data-230935021301/{esn}/2022-*")
            .select(to_timestamp("frm_gen_ts").alias("frm_gen_ts_modified"), "*")
            .write
            .option("mergeSchema", True)
            .mode('append')
            .format("delta")
            .saveAsTable("cummins.ngca")
        )
        print(f"Completed for - {esn}")
    except Exception as e:
        print(f"Completed for - {esn}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### View the data metadata in delta. The number of files and the total size in bytes for the table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc detail cummins.ngca

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize the table and do a z order by on the timestamp field that is used to query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize cummins.ngca zorder by (frm_gen_ts_modified);

# COMMAND ----------

# MAGIC %md
# MAGIC #### View the data metadata in delta after optimization. The number of files and the total size in bytes for the table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc detail cummins.ngca

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC Vacuum the table to remove unused files

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM cummins.ngca retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history cummins.ngca

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cummins.ngca

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/cummins.db/ngca"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/cummins.db/ngca/_delta_log/"))

# COMMAND ----------

df = spark.read.json("dbfs:/user/hive/warehouse/cummins.db/ngca/_delta_log/00000000000000000001.json")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query data based on timestamp and see if files are being pruned due the the z order statement

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cummins.ngca 
# MAGIC where frm_gen_ts_modified between to_timestamp('2021-06-09 16:00:00') and to_timestamp('2021-06-09 16:10:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC select componentSerialNumber,count(*) from cummins.ngca group by 1 order by 1 asc

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert from bronze history table to bronze streaming table

# COMMAND ----------


source_delta_df = spark.read.table("cummins.ngca")

df = source_delta_df.selectExpr(
    "case when componentSerialNumber is not null then componentSerialNumber else 'Blank' end as vEsn",
    "case when telematicsDeviceId is not null then telematicsDeviceId else 'Blank' end as vAvl",
    "case when frm_gen_ts is not null then substring(regexp_replace(frm_gen_ts, r'(T)', ' '), 0, length(frm_gen_ts)-5) else '2001-01-01 01:01:01' end as vDevTimeStamp",
    "case when frm_rcvd_ts is not null then substring(regexp_replace(frm_rcvd_ts, r'(T)', ' '), 0, length(frm_rcvd_ts)-5) else '2001-01-01 01:01:01' end as vSerTimeStamp",
    "case when comm_ts is not null then substring(regexp_replace(comm_ts, r'(T)', ' '), 0, length(comm_ts)-5) else '2001-01-01 01:01:01' end as vLCommunicationTimeStamp",
    "case when telematicsPartnerName is not null then telematicsPartnerName else 'N/A' end as vtelematicsPName",
    "case when totalEngineHour is not null then totalEngineHour else '0.0' end as vTotalEngHrs",
    "case when (totalEngineHour is not null and totalEngineHour not like '%N%') then cast(totalEngineHour as double) else -7777 end as dTotalEngHrs",
    "case when totalFuelConsumption is not null then totalFuelConsumption else '0.0' end as vTotalFuelUsed",
    "case when (totalFuelConsumption is not null and totalFuelConsumption not like '%N%') then cast(totalFuelConsumption as double) else -7777 end as dTotalFuelUsed",
    "concat('NGCA - ',(case when telematicsPartnerName is not null then telematicsPartnerName else 'N/A' end)) as vDataSourProviderName",
    "case when in_serv_loc is not null then cast(in_serv_loc as long) else 0 end as vIn_Service_Location",
    "explode(samples) as samples" 
  )

transformed_df = df.selectExpr(
    "vEsn as fGEId",
    "vAvl as fAvl",
    "vDevTimeStamp as fDeviceTimestamp",
    "to_timestamp(vDevTimeStamp, 'yyyy-MM-dd HH:mm:ss') as fDeviceTimestamp_modified",
    "vSerTimeStamp as fServerTimestamp",
    "vLCommunicationTimeStamp as fLCommunication",
    "vDataSourProviderName as fDSPName",
    "dTotalEngHrs as fTEHours",
    "dTotalFuelUsed as fTFUsed",
    "vIn_Service_Location as fIn_Service_Location",
    "case when samples.convertedDeviceParameters is not null then samples.convertedDeviceParameters.latitude else 'Blank' end as fLatitude",
    "case when samples.convertedDeviceParameters is not null then samples.convertedDeviceParameters.longitude else 'Blank' end as fLongitude"
  )

(
    transformed_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("cummins.silver_ngca_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cummins.silver_ngca_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cummins.ngca_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cummins.ngca

# COMMAND ----------


