-- Создание таблицы для доли отгрузки по подразделениям
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dm_Доли_Отгрузки' AND SCHEMA_NAME(schema_id) = 'dbo')
BEGIN
    CREATE TABLE dbo.dm_Доли_Отгрузки (
        БизнесРегион VARCHAR(36) NOT NULL,
        Год INT NOT NULL,
        Месяц INT NOT NULL,
        СуммаПродаж NUMERIC(15, 2) NOT NULL,
        ДоляМесяца DECIMAL(5, 4) NOT NULL,
        CONSTRAINT PK_dm_Доли_Отгрузки PRIMARY KEY (БизнесРегион, Год, Месяц)
    );
END;


-- Заполнение витрины доли отгрузки
WITH SalesByMonth AS (
    SELECT 
        p.БизнесРегион,
        YEAR(f.ДатаПродажи) AS Год,
        MONTH(f.ДатаПродажи) AS Месяц,
        SUM(f.ВыручкаСУчетомСкидокБезНалогов) AS СуммаПродаж
    FROM dbo.fact_Продажи f
    INNER JOIN dbo.ods_Документы_Продажи d 
        ON f.ДокументПродажи = d.Ссылка 
        AND d.is_active = 1
    INNER JOIN dbo.ods_Партнеры p 
        ON d.Партнер = p.Ссылка 
        AND p.is_active = 1
    WHERE f.ДатаПродажи >= DATEADD(YEAR, -3, GETDATE())
    GROUP BY p.БизнесРегион, YEAR(f.ДатаПродажи), MONTH(f.ДатаПродажи)
    HAVING SUM(f.ВыручкаСУчетомСкидокБезНалогов) > 0
),
TotalSales AS (
    SELECT 
        БизнесРегион,
        SUM(СуммаПродаж) AS ОбщаяСуммаПродаж
    FROM SalesByMonth
    GROUP BY БизнесРегион
    HAVING SUM(СуммаПродаж) > 0
)
INSERT INTO dbo.dm_Доли_Отгрузки (
    БизнесРегион,
    Год,
    Месяц,
    СуммаПродаж,
    ДоляМесяца
)
SELECT 
    s.БизнесРегион,
    s.Год,
    s.Месяц,
    s.СуммаПродаж,
    s.СуммаПродаж / t.ОбщаяСуммаПродаж AS ДоляМесяца
FROM SalesByMonth s
INNER JOIN TotalSales t 
    ON s.БизнесРегион = t.БизнесРегион;