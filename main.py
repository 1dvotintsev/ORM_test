
# Импортируем необходимые библиотеки
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, ForeignKey, func
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Подключение к базе данных
DATABASE_URL = "postgresql+psycopg2://superadmin:superadmin@localhost:5440/postgres"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()



class Country(Base):
    __tablename__ = 'countries'
    name = Column(String(40))
    country_id = Column(String(3), primary_key=True, unique=True)
    area_sqkm = Column(Integer)
    population = Column(Integer)


class Olympic(Base):
    __tablename__ = 'olympics'
    olympic_id = Column(String(7), primary_key=True, unique=True)
    country_id = Column(String(3), ForeignKey('countries.country_id'))
    city = Column(String(50))
    year = Column(Integer)
    startdate = Column(Date)
    enddate = Column(Date)
    country = relationship("Country")


class Player(Base):
    __tablename__ = 'players'
    name = Column(String(40))
    player_id = Column(String(10), primary_key=True, unique=True)
    country_id = Column(String(3), ForeignKey('countries.country_id'))
    birthdate = Column(Date)
    country = relationship("Country")


class Event(Base):
    __tablename__ = 'events'
    event_id = Column(String(7), primary_key=True, unique=True)
    name = Column(String(40))
    eventtype = Column(String(20))
    olympic_id = Column(String(7), ForeignKey('olympics.olympic_id'))
    is_team_event = Column(Integer)
    num_players_in_team = Column(Integer)
    result_noted_in = Column(String(100))
    olympic = relationship("Olympic")


class Result(Base):
    __tablename__ = 'results'
    event_id = Column(String(7), ForeignKey('events.event_id'), primary_key=True)
    player_id = Column(String(10), ForeignKey('players.player_id'), primary_key=True)
    medal = Column(String(7))
    result = Column(Float)
    event = relationship("Event")
    player = relationship("Player")


from sqlalchemy import func, case, extract

def task_1(session):
    query = session.query(
        extract('year', Player.birthdate).label('birth_year'),
        func.count(Player.player_id).label('num_players'),
        func.count().filter(Result.medal == 'GOLD').label('num_gold_medals')
    ).join(Result, Player.player_id == Result.player_id
    ).join(Event, Result.event_id == Event.event_id
    ).join(Olympic, Event.olympic_id == Olympic.olympic_id
    ).filter(Olympic.year == 2004
    ).group_by(extract('year', Player.birthdate))
    
    return query.all()



def task_2(session):
    query = (
        session.query(
            Event.name.label('event_name'),
            func.count(Result.player_id).label('num_players')
        )
        .join(Result, Event.event_id == Result.event_id)
        .filter(Event.is_team_event == 0, Result.medal == "GOLD")
        .group_by(Event.name)
        .having(func.count(Result.player_id) > 1)
    )
    return query.all()



def task_3(session):
    query = (
        session.query(
            Player.name.label('player_name'),
            Event.olympic_id.label('olympic_id')
        )
        .join(Result, Player.player_id == Result.player_id)
        .join(Event, Result.event_id == Event.event_id)
        .filter(Result.medal.in_(["GOLD", "SILVER", "BRONZE"]))
        .distinct()
    )
    return query.all()



from sqlalchemy import case

from sqlalchemy import case

def task_4(session):
    vowels = ["A", "E", "I", "O", "U"]
    subquery = (
        session.query(
            Player.country_id,
            func.count(Player.player_id).label('total_players'),
            func.sum(
                case(
                    (Player.name.op("~*")(r"^[AEIOU]"), 1),
                    else_=0
                )
            ).label('vowel_players')
        )
        .group_by(Player.country_id)
        .subquery()
    )

    query = (
        session.query(
            Country.name.label('country_name'),
            (subquery.c.vowel_players / subquery.c.total_players * 100).label('vowel_percentage')
        )
        .join(Country, Country.country_id == subquery.c.country_id)
        .order_by((subquery.c.vowel_players / subquery.c.total_players).desc())
        .limit(1)
    )

    result = query.one_or_none()
    
    return result


def task_5(session):
    # Подзапрос для подсчета медалей, используя связь через Player
    subquery = (
        session.query(
            Player.country_id,  # Ссылаемся на страну через Player
            func.count(Result.event_id).label('team_medals')
        )
        .join(Event, Event.event_id == Result.event_id)
        .filter(Event.is_team_event == 1, Event.olympic_id == "2000")
        .group_by(Player.country_id)  # Группируем по country_id в таблице Player
        .subquery()
    )

    # Основной запрос с использованием select_from для явного указания таблиц
    query = (
        session.query(
            Country.name.label('country_name'),
            (subquery.c.team_medals / Country.population).label('medals_to_population_ratio')
        )
        .select_from(Country)  # Указываем, что запрос начинается с таблицы Country
        .join(subquery, subquery.c.country_id == Country.country_id)  # Явное соединение с подзапросом
        .order_by((subquery.c.team_medals / Country.population).asc())
        .limit(5)
    )

    return query.all()





if __name__ == "__main__":
    session = Session()
    #print("Task 1:", task_1(session))
    
    print("Task 1:")
    print(f"{'Birth Year':<15} {'Num Players':<12} {'Num Gold Medals':<15}")
    for birth_year, num_players, num_gold_medals in task_1(session):
        print(f"{birth_year:<15} {num_players:<12} {num_gold_medals:<15}")
    
    #print("Task 2:", task_2(session))
    
    print("Task 2:")
    print(f"{'Event':<40} {'Count':<5}")
    for event, count in task_2(session):
        print(f"{event:<40} {count:<5}")
    
    #print("Task 3:", task_3(session))
    
    print("Task 3:")
    print(f"{'Player Name':<40} {'Olympic ID':<10}")
    for player_name, olympic_id in task_3(session):
        print(f"{player_name:<40} {olympic_id:<10}")
    
    
    result4 = task_4(session)
    print("Task 4:")
    print(f"{'Country Name':<30} {'Vowel Percentage':<15}")
    print(f"{result4.country_name:<30} {result4.vowel_percentage:.2f}%")
    
    
    print("Task 5:", task_5(session))
    session.close()
