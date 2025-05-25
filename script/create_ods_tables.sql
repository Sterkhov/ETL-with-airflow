-- Создание ODS-таблицы для Регистр_Продадж
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ods_Регистр_Продадж' AND SCHEMA_NAME(schema_id) = 'dbo')
BEGIN
    CREATE TABLE dbo.ods_Регистр_Продадж (
        НоменклатураID NVARCHAR(32) NOT NULL,
        ДокументID NVARCHAR(32) NOT NULL,
        ВыручкаСУчетомСкидокБезНалогов NUMERIC(15, 2) NOT NULL,
        СуммаКНП2Доход NUMERIC(15, 2) NOT NULL,
        СтоимостьЗакупкиБезНДС NUMERIC(15, 2) NOT NULL,
        НеликвиднаяПродажа VARCHAR(2) NOT NULL,
        ДокументTип VARCHAR(36),
        ДокументВид VARCHAR(36),
        ОбластьДанныхОсновныеДанные NUMERIC(7, 0) NOT NULL,
        Активность VARCHAR(2) NOT NULL,
        start_date DATETIME2(0) NOT NULL,
        end_date DATETIME2(0),
        is_active BIT NOT NULL,
        load_date DATETIME2(0) NOT NULL,
        CONSTRAINT PK_ods_Регистр_Продадж PRIMARY KEY (НоменклатураID, ДокументID, start_date)
    );
END;

-- Создание ODS-таблицы для Справочники_Партнеры
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ods_Партнеры' AND SCHEMA_NAME(schema_id) = 'dbo')
BEGIN
    CREATE TABLE dbo.ods_Партнеры (
        Ссылка VARCHAR(36) NOT NULL,
        Наименование NVARCHAR(100) NOT NULL,
        ПрофильКлиента NVARCHAR(100),
        БизнесРегион VARCHAR(36),
        ГруппаДоступа VARCHAR(36),
        мо_ДатаСозданияОбъекта DATETIME2(0),
        мо_ДатаИзмененияОбъекта DATETIME2(0),
        start_date DATETIME2(0) NOT NULL,
        end_date DATETIME2(0),
        is_active BIT NOT NULL,
        load_date DATETIME2(0) NOT NULL,
        CONSTRAINT PK_ods_Партнеры PRIMARY KEY (Ссылка, start_date)
    );
END;

-- Создание ODS-таблицы для Документы_Продажи
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ods_Документы_Продажи' AND SCHEMA_NAME(schema_id) = 'dbo')
BEGIN
    CREATE TABLE dbo.ods_Документы_Продажи (
        Ссылка VARCHAR(36) NOT NULL,
        Дата DATETIME2(0) NOT NULL,
        Партнер VARCHAR(36),
        Согласован VARCHAR(36),
        мо_ДатаСозданияОбъекта DATETIME2(0),
        мо_ДатаИзмененияОбъекта DATETIME2(0),
        start_date DATETIME2(0) NOT NULL,
        end_date DATETIME2(0),
        is_active BIT NOT NULL,
        load_date DATETIME2(0) NOT NULL,
        CONSTRAINT PK_ods_Документы_Продажи PRIMARY KEY (Ссылка, start_date)
    );
END;

-- Создание ODS-таблицы для Справочники_Номенклатура
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ods_Номенклатура' AND SCHEMA_NAME(schema_id) = 'dbo')
BEGIN
    CREATE TABLE dbo.ods_Номенклатура (
        Ссылка VARCHAR(36) NOT NULL,
        Наименование NVARCHAR(100) NOT NULL,
        start_date DATETIME2(0) NOT NULL,
        end_date DATETIME2(0),
        is_active BIT NOT NULL,
        load_date DATETIME2(0) NOT NULL,
        CONSTRAINT PK_ods_Номенклатура PRIMARY KEY (Ссылка, start_date)
    );
END;