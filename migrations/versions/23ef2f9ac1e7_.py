"""empty message

Revision ID: 23ef2f9ac1e7
Revises: 
Create Date: 2023-11-30 23:16:08.819480

"""
from alembic import op
import sqlalchemy as sa
from app import app, db
from sqlalchemy.ext.declarative import declarative_base


# revision identifiers, used by Alembic.
revision = '23ef2f9ac1e7'
down_revision = None
branch_labels = None
depends_on = None


Base = declarative_base()


class Result(db.Model):
    __bind_key__ = 'db_new'
    __tablename__ = 'result'

    id = db.Column(db.Integer, primary_key=True)
    test = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    lang = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    teststatus = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    dpalevel = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    ball100 = db.Column(db.DOUBLE_PRECISION(precision=50), autoincrement=False, nullable=True)
    ball12 = db.Column(db.INTEGER(), autoincrement=False, nullable=True)
    ball = db.Column(db.INTEGER(), autoincrement=False, nullable=True)
    adaptscale = db.Column(db.INTEGER(), autoincrement=False, nullable=True)


class PT(db.Model):
    __bind_key__ = 'db_new'
    __tablename__ = 'pt'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    regname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    areaname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    tername = db.Column(db.TEXT(), autoincrement=False, nullable=True)


class Reg(db.Model):
    __bind_key__ = 'db_new'
    __tablename__ = 'reg'

    id = db.Column(db.Integer, primary_key=True)
    regname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    areaname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    tername = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    tertypename = db.Column(db.TEXT(), autoincrement=False, nullable=True)


class Info(db.Model):
    __bind_key__ = 'db_new'
    __tablename__ = 'info'

    id = db.Column(db.Integer, primary_key=True)
    classprofilename = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    classlangname = db.Column(db.TEXT(), autoincrement=False, nullable=True)


class EO(db.Model):
    __bind_key__ = 'db_new'
    __tablename__ = 'eo'

    id = db.Column(db.Integer, primary_key=True)
    eoname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    eotypename = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    eoregname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    eoareaname = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    eotername = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    eoparent = db.Column(db.TEXT(), autoincrement=False, nullable=True)


class MainData2019(db.Model):
    __bind_key__ = 'db_new'
    __tablename__ = 'maindata2019'

    outid = db.Column(db.TEXT(), primary_key=True)
    birth = db.Column(db.INTEGER(), autoincrement=False, nullable=True)
    sextypename = db.Column(db.TEXT(), autoincrement=False, nullable=True)
    regtypename = db.Column(db.TEXT(), autoincrement=False, nullable=True)

    reg_id = db.Column(db.Integer(), db.ForeignKey('reg.id'), autoincrement=False, nullable=True)
    info_id = db.Column(db.Integer(), db.ForeignKey('info.id'), autoincrement=False, nullable=True)
    eo_id = db.Column(db.Integer(), db.ForeignKey('eo.id'), autoincrement=False, nullable=True)

    result_ukr_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_hist_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_math_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_phys_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_chem_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_bio_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_geo_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_eng_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_fra_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_deu_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)
    result_spa_id = db.Column(db.Integer(), db.ForeignKey('result.id'), autoincrement=False, nullable=True)

    pt_ukr_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_hist_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_math_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_phys_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_chem_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_bio_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_geo_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_eng_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_fra_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_deu_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)
    pt_spa_id = db.Column(db.Integer(), db.ForeignKey('pt.id'), autoincrement=False, nullable=True)


class Data2019(Base):
    __bind_key__ = 'db_old'
    __tablename__ = 'data2019'

    outid = db.Column(db.Text, primary_key=True)
    birth = db.Column(db.Integer)
    sextypename = db.Column(db.Text)
    regname = db.Column(db.Text)
    areaname = db.Column(db.Text)
    tername = db.Column(db.Text)
    regtypename = db.Column(db.Text)
    tertypename = db.Column(db.Text)
    classprofilename = db.Column(db.Text)
    classlangname = db.Column(db.Text)
    eoname = db.Column(db.Text)
    eotypename = db.Column(db.Text)
    eoregname = db.Column(db.Text)
    eoareaname = db.Column(db.Text)
    eotername = db.Column(db.Text)
    eoparent = db.Column(db.Text)
    ukrtest = db.Column(db.Text)
    ukrteststatus = db.Column(db.Text)
    ukrball100 = db.Column(db.Float)
    ukrball12 = db.Column(db.Float)
    ukrball = db.Column(db.Float)
    ukradaptscale = db.Column(db.Integer)
    ukrptname = db.Column(db.Text)
    ukrptregname = db.Column(db.Text)
    ukrptareaname = db.Column(db.Text)
    ukrpttername = db.Column(db.Text)
    histtest = db.Column(db.Text)
    histlang = db.Column(db.Text)
    histteststatus = db.Column(db.Text)
    histball100 = db.Column(db.Float)
    histball12 = db.Column(db.Float)
    histball = db.Column(db.Float)
    histptname = db.Column(db.Text)
    histptregname = db.Column(db.Text)
    histptareaname = db.Column(db.Text)
    histpttername = db.Column(db.Text)
    mathtest = db.Column(db.Text)
    mathlang = db.Column(db.Text)
    mathteststatus = db.Column(db.Text)
    mathball100 = db.Column(db.Float)
    mathball12 = db.Column(db.Float)
    mathball = db.Column(db.Float)
    mathptname = db.Column(db.Text)
    mathptregname = db.Column(db.Text)
    mathptareaname = db.Column(db.Text)
    mathpttername = db.Column(db.Text)
    phystest = db.Column(db.Text)
    physlang = db.Column(db.Text)
    physteststatus = db.Column(db.Text)
    physball100 = db.Column(db.Float)
    physball12 = db.Column(db.Float)
    physball = db.Column(db.Float)
    physptname = db.Column(db.Text)
    physptregname = db.Column(db.Text)
    physptareaname = db.Column(db.Text)
    physpttername = db.Column(db.Text)
    chemtest = db.Column(db.Text)
    chemlang = db.Column(db.Text)
    chemteststatus = db.Column(db.Text)
    chemball100 = db.Column(db.Float)
    chemball12 = db.Column(db.Float)
    chemball = db.Column(db.Float)
    chemptname = db.Column(db.Text)
    chemptregname = db.Column(db.Text)
    chemptareaname = db.Column(db.Text)
    chempttername = db.Column(db.Text)
    biotest = db.Column(db.Text)
    biolang = db.Column(db.Text)
    bioteststatus = db.Column(db.Text)
    bioball100 = db.Column(db.Float)
    bioball12 = db.Column(db.Float)
    bioball = db.Column(db.Float)
    bioptname = db.Column(db.Text)
    bioptregname = db.Column(db.Text)
    bioptareaname = db.Column(db.Text)
    biopttername = db.Column(db.Text)
    geotest = db.Column(db.Text)
    geolang = db.Column(db.Text)
    geoteststatus = db.Column(db.Text)
    geoball100 = db.Column(db.Float)
    geoball12 = db.Column(db.Float)
    geoball = db.Column(db.Float)
    geoptname = db.Column(db.Text)
    geoptregname = db.Column(db.Text)
    geoptareaname = db.Column(db.Text)
    geopttername = db.Column(db.Text)
    engtest = db.Column(db.Text)
    engteststatus = db.Column(db.Text)
    engball100 = db.Column(db.Float)
    engball12 = db.Column(db.Float)
    engdpalevel = db.Column(db.Text)
    engball = db.Column(db.Float)
    engptname = db.Column(db.Text)
    engptregname = db.Column(db.Text)
    engptareaname = db.Column(db.Text)
    engpttername = db.Column(db.Text)
    fratest = db.Column(db.Text)
    frateststatus = db.Column(db.Text)
    fraball100 = db.Column(db.Float)
    fraball12 = db.Column(db.Float)
    fradpalevel = db.Column(db.Text)
    fraball = db.Column(db.Float)
    fraptname = db.Column(db.Text)
    fraptregname = db.Column(db.Text)
    fraptareaname = db.Column(db.Text)
    frapttername = db.Column(db.Text)
    deutest = db.Column(db.Text)
    deuteststatus = db.Column(db.Text)
    deuball100 = db.Column(db.Float)
    deuball12 = db.Column(db.Float)
    deudpalevel = db.Column(db.Text)
    deuball = db.Column(db.Float)
    deuptname = db.Column(db.Text)
    deuptregname = db.Column(db.Text)
    deuptareaname = db.Column(db.Text)
    deupttername = db.Column(db.Text)
    spatest = db.Column(db.Text)
    spateststatus = db.Column(db.Text)
    spaball100 = db.Column(db.Float)
    spaball12 = db.Column(db.Float)
    spadpalevel = db.Column(db.Text)
    spaball = db.Column(db.Float)
    spaptname = db.Column(db.Text)
    spaptregname = db.Column(db.Text)
    spaptareaname = db.Column(db.Text)
    spapttername = db.Column(db.Text)


with app.app_context():
    db.create_all()


def fill_required_lines(model, col_names):
    # Очищуємо попередні дані, щоб не було повторень
    db.session.query(model).delete()
    db.session.commit()

    # Отримуємо необхідні дані
    necessary_columns = [getattr(Data2019, column) for column in col_names]
    records = db.session.query(*necessary_columns).distinct().all()

    # Проходимось по кожному запису
    for record in records:
        column_value_dict = {col_names[i]: getattr(record, col_names[i]) for i in range(len(col_names))}

        # Обєкт для додавання даних у модель
        model_record = model(**column_value_dict)
        db.session.add(model_record)

    db.session.commit()
    print(f"Filled {model.__tablename__}")


def fill_pt(subjects):
    print(f"\nStart fill PT:")
    for sub in subjects:
        start_col_name = sub + "pt"
        pt_columns = [attr for attr, val in vars(Data2019).items() if start_col_name in attr]

        necessary_columns = [getattr(Data2019, column) for column in pt_columns]
        records = db.session.query(*necessary_columns).distinct().all()

        for record in records:
            column_value_dict = {pt_columns[i][len(sub)+2:]: getattr(record, pt_columns[i]) for i in range(len(pt_columns))}

            existing_record = db.session.query(PT).filter_by(**column_value_dict).first()
            if existing_record is None:
                model_record = PT(**column_value_dict)
                db.session.add(model_record)

        db.session.commit()
        print(f"Filled PT ({sub})")

    print("-"*12)
    print(f"Filled PT")


def fill_result(subjects):
    print(f"\nStart fill Result:")
    for sub in subjects:
        pt_columns = [attr for attr, val in vars(Data2019).items() if sub in attr and sub+'pt' not in attr]
        pt_columns_without_sub = [col[len(sub):] for col in pt_columns]
        columns_from_result = Result.__table__.columns.keys()[2:]

        # Дізнаємось яких колонок немає, вони повинні бути None
        must_be_none = list(set(columns_from_result) - set(pt_columns_without_sub))

        necessary_columns = [getattr(Data2019, column) for column in pt_columns]
        records = db.session.query(*necessary_columns).distinct().all()

        for record in records:
            column_value_dict = {pt_columns[i][len(sub):]: getattr(record, pt_columns[i]) for i in range(len(pt_columns))}

            for col in must_be_none:
                column_value_dict[col] = None

            # Обєкт для додавання даних у модель
            model_record = Result(**column_value_dict)
            db.session.add(model_record)

        db.session.commit()
        print(f"Filled Result ({sub})")

    print("-"*12)
    print(f"Filled Result")


simple_tables = [
        {"col_names": Reg.__table__.columns.keys()[1:], "model": Reg, "foreign_key": "reg_id"},
        {"col_names": Info.__table__.columns.keys()[1:], "model": Info, "foreign_key": "info_id"},
        {"col_names": EO.__table__.columns.keys()[1:], "model": EO, "foreign_key": "eo_id"}
    ]


def fill_MainData2019(subjects):
    print(f"\nStart fill MainData2019:")

    col_names_from_maindata2019 = MainData2019.__table__.columns.keys()
    simple_keys = col_names_from_maindata2019[:4]  # col_names_from_maindata2019_with_simple_keys
    foreign_keys = col_names_from_maindata2019[4:]  # col_names_from_maindata2019_with_foreign_keys
    all_col_names = Data2019.__table__.columns.keys()

    records = db.session.query(Data2019).all()

    col_names_from_result = Result.__table__.columns.keys()[2:]
    col_names_from_pt = PT.__table__.columns.keys()[2:]

    # Проходимось по кожному запису
    iter = 0
    for record in records:
        if iter % 50 == 0 and iter != 0:
            print(f"Filled {iter} rows")
        column_value_dict = {simple_keys[i]: getattr(record, simple_keys[i]) for i in range(len(simple_keys))}

        # for el in all_col_names:
        #     print(getattr(record, el), end=' ')

        # Reg Info EO
        for item in simple_tables:
            # Отримуємо значення, які повинні бути
            necessary_columns_value = [getattr(record, column) for column in item['col_names']]
            # Робимо словник {назва колонки: значення}
            necessary_dict = dict(zip(item['col_names'], necessary_columns_value))
            # Створюємо фільтер
            filters = [getattr(item['model'], col_name) == necessary_dict[col_name] for col_name in necessary_dict]
            # Шукаємо потрібний рядок у відповідній моделі
            model_record = db.session.query(item['model']).filter(*filters).first()
            # Записуємо отримане id у словник
            column_value_dict[item['foreign_key']] = model_record.id

        # PT
        for sub in subjects:
            current_col_names = [sub+'pt'+col for col in col_names_from_pt]
            # Отримуємо значення, які повинні бути
            necessary_columns_value = [getattr(record, column) for column in current_col_names]
            # Робимо словник {назва колонки: значення}
            necessary_dict = dict(zip(col_names_from_pt, necessary_columns_value))
            # Створюємо фільтер
            filters = [getattr(PT, col_name) == necessary_dict[col_name] for col_name in necessary_dict]
            # Шукаємо потрібний рядок у відповідній моделі
            model_record = db.session.query(PT).filter(*filters).first()
            # Записуємо отримане id у словник
            column_value_dict['pt_'+sub+'_id'] = model_record.id

        # Result
        for sub in subjects:
            current_col_names = [sub+col for col in col_names_from_result]
            intersection = list(set(all_col_names) & set(current_col_names))  # Колонки які є в Data2019
            difference = list(set(current_col_names) - set(all_col_names))    # Колонки, який немає у Data2019, але э у Result
            # Отримуємо значення, які повинні бути
            necessary_columns_value = [getattr(record, column) for column in intersection]
            # Робимо словник {назва колонки: значення}
            necessary_dict = dict(zip(intersection, necessary_columns_value))
            for diff in difference:
                necessary_dict[diff] = None
            # Створюємо фільтер
            filters = [getattr(Result, col_name[len(sub):]) == necessary_dict[col_name] for col_name in necessary_dict]
            # Шукаємо потрібний рядок у відповідній моделі
            model_record = db.session.query(Result).filter(*filters).first()
            # Записуємо отримане id у словник
            column_value_dict['result_'+sub+'_id'] = model_record.id

        # Обєкт для додавання даних у модель
        model_record = MainData2019(**column_value_dict)
        db.session.add(model_record)

        iter += 1

    db.session.commit()
    print(f"Loaded {iter} rows")
    print("-"*12)
    print("Filled MainData2019")


def upgrade():
    print("upgrade")

    for item in simple_tables:
        fill_required_lines(item['model'], item['col_names'])

    subjects = ["ukr", "hist", "math", "phys", "chem", "bio", "geo", "eng", "fra", "deu", "spa"]
    fill_pt(subjects)
    fill_result(subjects)
    fill_MainData2019(subjects)


def downgrade():
    print("downgrade")
    db.drop_all()
