-- Создание таблицы fact_Продажи
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_Продажи' AND SCHEMA_NAME(schema_id) = 'dbo')
BEGIN
    CREATE TABLE dbo.fact_Продажи (
        ДокументПродажи VARCHAR(36) NOT NULL,
        Партнер NVARCHAR(100) NOT NULL,
        Номенклатура NVARCHAR(100) NOT NULL,
        ВыручкаСУчетомСкидокБезНалогов NUMERIC(15, 2) NOT NULL,
        СтоимостьЗакупкиБезНДС NUMERIC(15, 2) NOT NULL,
        СуммаКНП2Доход NUMERIC(15, 2) NOT NULL,
        Прибыль NUMERIC(15, 2) NOT NULL,
        ДатаПродажи DATETIME2(0) NOT NULL,
        CONSTRAINT PK_fact_Продажи PRIMARY KEY (ДокументПродажи, Номенклатура)
    );
END;

-- Заполнение витрины fact_Продажи
INSERT INTO dbo.fact_Продажи (
    ДокументПродажи,
    Партнер,
    Номенклатура,
    ВыручкаСУчетомСкидокБезНалогов,
    СтоимостьЗакупкиБезНДС,
    СуммаКНП2Доход,
    Прибыль,
    ДатаПродажи
)
SELECT 
    r.ДокументID AS ДокументПродажи,
    COALESCE(p.Наименование, 'Партнер не указан') AS Партнер,
    n.Наименование AS Номенклатура,
    r.ВыручкаСУчетомСкидокБезНалогов,
    r.СтоимостьЗакупкиБезНДС,
    r.СуммаКНП2Доход,
    (r.ВыручкаСУчетомСкидокБезНалогов - r.СтоимостьЗакупкиБезНДС + r.СуммаКНП2Доход) AS Прибыль,
    d.Дата AS ДатаПродажи
FROM dbo.ods_Регистр_Продадж r
INNER JOIN dbo.ods_Документы_Продажи d 
    ON r.ДокументID = d.Ссылка 
    AND d.is_active = 1
LEFT JOIN dbo.ods_Партнеры p 
    ON d.Партнер = p.Ссылка 
    AND p.is_active = 1
INNER JOIN dbo.ods_Номенклатура n 
    ON r.НоменклатураID = n.Ссылка 
    AND n.is_active = 1
WHERE r.is_active = 1
    AND r.НеликвиднаяПродажа != '00'
    AND (p.ПрофильКлиента IS NULL OR p.ПрофильКлиента != 'МГО');