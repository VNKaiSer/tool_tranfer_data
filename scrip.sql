CREATE TABLE CustomsDeclaration (
    Id BIGINT auto_increment PRIMARY KEY,
    CDS VARCHAR(255),
    CustomsOffice VARCHAR(50),
    TPSMode VARCHAR(10),
    TradeType VARCHAR(10),
    DATE DATE,
    HOUR TIME,
    ERC VARCHAR(50),
    Exportor VARCHAR(255),
    Importor VARCHAR(255),
    ImportCountry VARCHAR(50),
    BLNumber VARCHAR(50),
    Quantity INT,
    UOM VARCHAR(10),
    GWeight DECIMAL(10, 2),
    WeightUOM VARCHAR(10),
    FinalDestination VARCHAR(255),
    POL VARCHAR(50),
    Value DECIMAL(15, 2),
    TaxableValue DECIMAL(15, 2),
    TaxValue DECIMAL(15, 2),
    Note TEXT,
    CDSCompletedDate DATE,
    CDSCompletedHour TIME,
    CDSCancelDate DATE,
    CDSCancelHour TIME,
    Officer VARCHAR(100),
    Officer2 VARCHAR(100),
    HSCode VARCHAR(50),
    Commodity VARCHAR(255),
    UnitQuantity INT,
    UnitCost DECIMAL(10, 2),
    InvoiceBL VARCHAR(50),
    Currency VARCHAR(10),
    InvoiceValue DECIMAL(15, 2),
    TaxableValue2 DECIMAL(15, 2),
    TaxRate DECIMAL(5, 2),
    TaxClass VARCHAR(50),
    Tax DECIMAL(15, 2),
    RefDoc1 VARCHAR(100),
    RefDoc2 VARCHAR(100)
) 
PARTITION BY RANGE (TO_DAYS(DATE)) (
    -- 2023
    PARTITION p2023_q1 VALUES LESS THAN (TO_DAYS('2023-04-01')),
    PARTITION p2023_q2 VALUES LESS THAN (TO_DAYS('2023-07-01')),
    PARTITION p2023_q3 VALUES LESS THAN (TO_DAYS('2023-10-01')),
    PARTITION p2023_q4 VALUES LESS THAN (TO_DAYS('2024-01-01')),

    -- 2024
    PARTITION p2024_q1 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p2024_q2 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p2024_q3 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p2024_q4 VALUES LESS THAN (TO_DAYS('2025-01-01')),

    -- 2025
    PARTITION p2025_q1 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p2025_q2 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p2025_q3 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p2025_q4 VALUES LESS THAN (TO_DAYS('2026-01-01')),

    -- 2026
    PARTITION p2026_q1 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026_q2 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026_q3 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026_q4 VALUES LESS THAN (TO_DAYS('2027-01-01')),

    -- 2027
    PARTITION p2027_q1 VALUES LESS THAN (TO_DAYS('2027-04-01')),
    PARTITION p2027_q2 VALUES LESS THAN (TO_DAYS('2027-07-01')),
    PARTITION p2027_q3 VALUES LESS THAN (TO_DAYS('2027-10-01')),
    PARTITION p2027_q4 VALUES LESS THAN (TO_DAYS('2028-01-01')),

    -- 2028
    PARTITION p2028_q1 VALUES LESS THAN (TO_DAYS('2028-04-01')),
    PARTITION p2028_q2 VALUES LESS THAN (TO_DAYS('2028-07-01')),
    PARTITION p2028_q3 VALUES LESS THAN (TO_DAYS('2028-10-01')),
    PARTITION p2028_q4 VALUES LESS THAN (TO_DAYS('2029-01-01')),

    -- 2029
    PARTITION p2029_q1 VALUES LESS THAN (TO_DAYS('2029-04-01')),
    PARTITION p2029_q2 VALUES LESS THAN (TO_DAYS('2029-07-01')),
    PARTITION p2029_q3 VALUES LESS THAN (TO_DAYS('2029-10-01')),
    PARTITION p2029_q4 VALUES LESS THAN (TO_DAYS('2030-01-01')),

    -- 2030
    PARTITION p2030_q1 VALUES LESS THAN (TO_DAYS('2030-04-01')),
    PARTITION p2030_q2 VALUES LESS THAN (TO_DAYS('2030-07-01')),
    PARTITION p2030_q3 VALUES LESS THAN (TO_DAYS('2030-10-01')),
    PARTITION p2030_q4 VALUES LESS THAN (TO_DAYS('2031-01-01'))
);