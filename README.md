# clan-raid  

Попробуем создать систему отчетности для отслеживания активности игроков в игре Clash of clans.
## Причина создания системы отчетности
Несколько месяцев назад я решил скачать одну игру стратегию, которую играл давным давно. Эта игра называется "Clash of Clans", где игроки улучшают свою деревню, атакуя вражеские деревни и участвуя в различных событиях, после вступления в какой-то клан. Когда ты находишься в клане, каждую неделю на протяжении 3 дней можешь участвовать в так называемых рейдах и получать за эту валюту, которую можно тратить на улучшение столичных деревень (общие деревни для всего клана). В любом клане "глава" хочет, чтобы каждый игрок поучаствовал в рейдах, и для этого ему или соруководителю клана приходится каждый раз смотреть игровой список атак и сверяться с списком участников клана. Делать это вручную конечно муторно и долго, к тому же приятно было бы иметь некоторую визуальную статистику, чтобы следить за уровнем игры игроков. Тогда я решил попробовать создать систему отчетности для отслеживания участия игроков в рейдах.
## Источник данных
Для построния системы отчетности, нужны данные. Для этого я решил использовать API из официального сайта игры: [Clash of Calns API](https://developer.clashofclans.com/#/), где нужно пройти регистрацию и создать API токен для возможности скачивания JSON файлов с данными об клане.  
  
<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/API_my_account.png" width="520">  
  
После регистрации заходим в раздел "My Account" и создаем API ключ, указав имя, описание и допускаемые IP адреса для этого токена, который можем скопировать, нажав но созданный ключ.  
  
<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/API_create_key.png" width="600">  
  
<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/API_key_example.png" width="600">  

После этого можем перейти в раздел "Documentation" рассмотреть какие данные можем забрать из этого сайта. Мы будем брать информацию об участниках клана (`/clans/{clanTag}/members/`) и о рейдах (`/clans/{clanTag}/capitalraidseasons/`).  

<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/API_documentation.png" width="600">  
  
  
Теперь нужно создать БД, где будем хранить наши данные. Будем использовать СУБД PostgreSQL, который вполне достаточно для нашей задачи. У нас будет 4 таблицы:  
- `members` - будет храниться информация про игроков клана.
    - `tag`. Тег игрока. Тип `varchar`. Первичный ключ.
    - `name`. Ник игрока. Тип `varchar`.
    - `role`. Статус игрока в клане. Тип `varchar`.
    - `trophies`. Количество трофеев игрока. Тип `int`.
- `raid` - будет храниться общая информация про рейд.
    - `raid_id`. Идентификатор рейда. Будем использовать для него объект `sequence`, чтобы оно автоматический генерировало id. Первичный ключ.
    - `state`. Состояние рейда. Тип `varchar`.
    - `starrtime`. Время начала рейда. Тип `timestamp`.
    - `endtime`. Время окончания рейда. Тип `timestamp`.
    - `capitaltotalloot`. Общая добыча за рейд. Тип `int`.
    - `raidscompleted`. Количество совершенных рейдов в столичные столицы соперников. Тип `int`.
    - `districtdestroyed`. Количество уничтоженных районов столицы. Тип `int`.
- `raids_attacks`- будет храниться информация про атаки игроков в каждом рейде.
    - `raid_id`. Идентификатор рейда. Тип `int`. Внешний ключ.
    - `tag`. Тег игрока. Тип `varchar`.
    - `attacks`. Количество атак игрока. Тип `int`.
    - `bonusattacklimit`. Есть бонусная атака или нет. Тип `int`.
    - `resourceslooted`. Добыча игрока в этом рейде. Тип `int`.
- `unattacked_players` - будет храниться информация про игроков, которые не атаковали в рейдах
    - `tag`. Тег игрока. Тип `varchar`.
    - `raid_id`. Идентификатор рейда, в котором не участвовал игрок. Тип `int`.

В конечном итоге получим:  
  
<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/data_model.png" width="600">  
  
Осталось написать DAG, который загружает, обрабатывает и загружает данные в БД и Google Sheet. Код написан [тут](https://github.com/f0rest-mAker/DataLearn/blob/main/DE-101/Module4/Airflow%20DAGs/clan_info.py). DAG выглядит следующим образом:  

<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/Clash_DAG.png" width="1000">  

Может появится вопрос, зачем сохранять в Google Sheet, если данные уже загружаются в БД? Делаем это из-за того, что моя БД находится на локальном сервере, а Tableau Desktop, не может подключаться к локальным базам данных, поэтому приходится сохранять их в Sheets, чтобы данные дошли до стадии визуализации. Таким образом, нам хватит нажать 1 кнопку, чтобы обновить данные в BI инструменте.  
Google Sheet документ выглядит следующим образом:  
  
<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/GoogleSheets.png" width="600"> 
  
Итак, DAG состоит из входа в граф (`DummyOperator`), трех групп задач, каждый из которых делает нужную логику обработки данных, а также выхода, который отвечает за закрытие поключения.  
Логика работы DAGа такова:
1) Сначала запускается группа задач, которая отвечает за скачивание JSON файлов с помощью API и их сохранение в локальной машине.
2) После этого последовательно выполняются задачи из групп по работе с участниками клана и с информацией про рейды. Очередь начинается с группы задач по работе с участниками клана. В этих задачах происходит чтение JSON файлов, выделение нужных объектов, их сохранение / обновление в БД и Google Sheet.  
  Стоит отметить, как обновляются данные про рейды. Если окажется, что в БД нет никаких данных про рейды, то мы добаляем туда последний активный рейд. Если текущее время меньше конца последнего рейда, то обновляем имеющиеся данные про этот рейд. Если же окажется, что текущее время больше времени окончания рейда, то мы "закрываем" этот рейд (ставим статус "ended", делаем последнее обновление). Если время последнего обновления меньше времени окончания рейда, то обновляем данные текущего рейда на актуальные. Если окажется, что текущее время находится между временами начала и конца рейда, то добавляем новую информацию про рейд. В любом ином случае просто пропускаем все последующие задачи этой группы.
3) После выполнения группы задач, закрывает подключение.  

### Tableau
Теперь осталось сделать визуализацию наших данных. Подключимся в нашему Google Sheet и создадим модель данных в Tableau.  

<img src="https://github.com/f0rest-mAker/DataLearn/blob/main/DE-101/Module4/img/DashBoard_DataModel.png" width="600">  
  
Определимся, что нам нужно в первую очередь.  
1) Основные KPI для рейда: добыча, количество атак, количество атакованных районов вражеских кланов за рейд.
2) Игроки, которые не атаковали в указанном рейде.
3) Динамика общей добычи клана по рейдам.
4) Информация про атаки игроков.
5) Прогресс выполнения плана по добыче.

Дополнительно можно сделать общую статистику игроков за все время рейдов и указать, в каком месте среди клана они находятся по этом показателям. Дальше добавим интерактивность в дашборд: возможность просмотра информации про выбранный рейд, возможность просмотра статистики конкретных игроков.  
Также добавим маленькую документацию для пользователей, которую можно открыть, нажав на кнопку документа в правом верхнем углу дашборда.  
В конечном итоге получим следующий дашборд:  
  
<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/DashBoard_1.png" width="1000">  

<img src="https://github.com/f0rest-mAker/clan-raid/blob/main/img/DashBoard_2.png" width="1000">  
   
Ссылка на [дашборд](https://public.tableau.com/views/ClanStatistics/ClanRaids) в Tableau Public. Однако, тут отстутсвуют некоторые рейды из-за моей невнимательности, так как у меня либо не было времени запускать сервер Airflow, либо я просто забывал это делать. А если бы был постоянный сервер, то можно было просто поставить на расписание обновления в airflow, и не думать об постоянном ручном запуске локального сервера.
## Итоги
Таким образом, я попробовал создать систему отчетности, которая даёт возможность легко и быстро просматривать активность клана в рейдах, статистику игроков, на основе которых можно решать, оставлять этого игрока или искать вместо него замену, чтобы улучшить добычи в рейдах. Благодаря Airflow можно задать интервал обновления данных, что даст возможность всегда в указанное время иметь свежие данные.  
## Планы на будущее
В дальнейшем хотелось бы изменить модель данных, чтобы сохранялась некоторая историчность данных, на данный момент во время срабатывания DAG все старые данные с members просто удаляются и добавляются свежие, из-за чего там будут только те игроки, которые состоят на данный момент в клане. Также такое решение позволит нам определить тег игрока в таблице `members` как первичный ключ, пока это нельзя сделать, так как игроки, которые вышли из клана, удаляются с таблицы, что приведет к нарушению ссылочной целостности с другими таблицами.
