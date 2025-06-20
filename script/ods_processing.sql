-- Обработка ODS-таблицы для Регистр_Продадж
MERGE INTO dbo.ods_Регистр_Продадж AS target
USING (
    SELECT 
        НоменклатураID, 
        ДокументID, 
        ВыручкаСУчетомСкидокБезНалогов, 
        СуммаКНП2Доход, 
        СтоимостьЗакупкиБезНДС, 
        НеликвиднаяПродажа, 
        ДокументTип, 
        ДокументВид, 
        ОбластьДанныхОсновныеДанные, 
        Активность, 
        load_date
    FROM dbo.stage_Регистр_Продадж
    WHERE load_date = (SELECT MAX(load_date) FROM dbo.stage_Регистр_Продадж)
) AS source
ON target.НоменклатураID = source.НоменклатураID 
   AND target.ДокументID = source.ДокументID 
   AND target.is_active = 1
WHEN MATCHED AND (
    target.ВыручкаСУчетомСкидокБезНалогов != source.ВыручкаСУчетомСкидокБезНалогов OR
    target.СуммаКНП2Доход != source.СуммаКНП2Доход OR
    target.СтоимостьЗакупкиБезНДС != source.СтоимостьЗакупкиБезНДС OR
    target.НеликвиднаяПродажа != source.НеликвиднаяПродажа OR
    target.ДокументTип != source.ДокументTип OR
    target.ДокументВид != source.ДокументВид OR
    target.ОбластьДанныхОсновныеДанные != source.ОбластьДанныхОсновныеДанные OR
    target.Активность != source.Активность
) THEN
    UPDATE SET 
        end_date = GETDATE(),
        is_active = 0
WHEN NOT MATCHED THEN
    INSERT (
        НоменклатураID, ДокументID, ВыручкаСУчетомСкидокБезНалогов, 
        СуммаКНП2Доход, СтоимостьЗакупкиБезНДС, НеликвиднаяПродажа, 
        ДокументTип, ДокументВид, ОбластьДанныхОсновныеДанные, 
        Активность, start_date, end_date, is_active, load_date
    )
    VALUES (
        source.НоменклатураID, source.ДокументID, source.ВыручкаСУчетомСкидокБезНалогов, 
        source.СуммаКНП2Доход, source.СтоимостьЗакупкиБезНДС, source.НеликвиднаяПродажа, 
        source.ДокументTип, source.ДокументВид, source.ОбластьДанныхОсновныеДанные, 
        source.Активность, GETDATE(), NULL, 1, source.load_date
    );

-- Обработка ODS-таблицы для Партнеры
MERGE INTO dbo.ods_Партнеры AS target
USING (
    SELECT 
        Ссылка, 
        Наименование, 
        ПрофильКлиента, 
        БизнесРегион, 
        ГруппаДоступа, 
        мо_ДатаСозданияОбъекта, 
        мо_ДатаИзмененияОбъекта, 
        load_date
    FROM dbo.stage_Справочники_Партнеры
    WHERE load_date = (SELECT MAX(load_date) FROM dbo.stage_Справочники_Партнеры)
) AS source
ON target.Ссылка = source.Ссылка 
   AND target.is_active = 1
WHEN MATCHED AND (
    target.Наименование != source.Наименование OR
    target.ПрофильКлиента != source.ПрофильКлиента OR
    target.БизнесРегион != source.БизнесРегион OR
    target.ГруппаДоступа != source.ГруппаДоступа OR
    target.мо_ДатаСозданияОбъекта != source.мо_ДатаСозданияОбъекта OR
    target.мо_ДатаИзмененияОбъекта != source.мо_ДатаИзмененияОбъекта
) THEN
    UPDATE SET 
        end_date = GETDATE(),
        is_active = 0
WHEN NOT MATCHED THEN
    INSERT (
        Ссылка, Наименование, ПрофильКлиента, 
        БизнесРегион, ГруппаДоступа, 
        мо_ДатаСозданияОбъекта, мо_ДатаИзмененияОбъекта, 
        start_date, end_date, is_active, load_date
        )
    VALUES (
        source.Ссылка, source.Наименование, source.ПрофильКлиента, 
        source.БизнесРегион, source.ГруппаДоступа, 
        source.мо_ДатаСозданияОбъекта, source.мо_ДатаИзмененияОбъекта, 
        GETDATE(), NULL, 1, source.load_date
    );

-- Обработка ODS-таблицы для Документы_Продажи
MERGE INTO dbo.ods_Документы_Продажи AS target
USING (
    SELECT 
        Ссылка, 
        Дата, 
        Партнер, 
        Согласован, 
        мо_ДатаСозданияОбъекта, 
        мо_ДатаИзмененияОбъекта, 
        load_date
    FROM dbo.stage_Документы_Продажи
    WHERE load_date = (SELECT MAX(load_date) FROM dbo.stage_Документы_Продажи)
) AS source
ON target.Ссылка = source.Ссылка 
   AND target.is_active = 1
WHEN MATCHED AND (
    target.Дата != source.Дата OR
    target.Партнер != source.Партнер OR
    target.Согласован != source.Согласован OR
    target.мо_ДатаСозданияОбъекта != source.мо_ДатаСозданияОбъекта OR
    target.мо_ДатаИзмененияОбъекта != source.мо_ДатаИзмененияОбъекта
) THEN
    UPDATE SET 
        end_date = GETDATE(),
        is_active = 0
WHEN NOT MATCHED THEN
    INSERT (
        Ссылка, Дата, Партнер, Согласован, 
        мо_ДатаСозданияОбъекта, мо_ДатаИзмененияОбъекта, 
        start_date, end_date, is_active, load_date
    )
    VALUES (
        source.Ссылка, source.Дата, source.Партнер, source.Согласован, 
        source.мо_ДатаСозданияОбъекта, source.мо_ДатаИзмененияОбъекта, 
        GETDATE(), NULL, 1, source.load_date
    );

-- Обработка ODS-таблицы для Номенклатура
MERGE INTO dbo.ods_Номенклатура AS target
USING (
    SELECT 
        Ссылка, 
        Наименование, 
        load_date
    FROM dbo.stage_Справочники_Номенклатура
    WHERE load_date = (SELECT MAX(load_date) FROM dbo.stage_Справочники_Номенклатура)
) AS source
ON target.Ссылка = source.Ссылка 
   AND target.is_active = 1
WHEN MATCHED AND (
    target.Наименование != source.Наименование
) THEN
    UPDATE SET 
        end_date = GETDATE(),
        is_active = 0
WHEN NOT MATCHED THEN
    INSERT (
        Ссылка, Наименование, start_date, end_date, is_active, load_date
    )
    VALUES (
        source.Ссылка, source.Наименование, GETDATE(), NULL, 1, source.load_date
    );