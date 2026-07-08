@echo off
REM setup.bat — Create the SQLite database and populate it with 50 sample product rows.
REM
REM Usage:
REM   setup.bat
REM
REM Prerequisites:
REM   - sqlite3.exe must be installed and on your PATH.
REM     Download from https://www.sqlite.org/download.html
REM     (look for "sqlite-tools-win-x64-*.zip", extract, add directory to PATH)
REM   - Run from the directory that contains this script (09-jdbc-source-sqlite\).
REM
REM What this script does:
REM   1. Creates the data\ directory if it does not exist.
REM   2. Creates (or recreates) .\data\inventory.db.
REM   3. Creates the 'products' table.
REM   4. Inserts 50 realistic product rows across 5 categories.
REM
REM After running this script, execute run.bat to start the pipeline.

setlocal enabledelayedexpansion

REM ── Move to the directory that contains this script ───────────────────────
cd /d "%~dp0"

REM ── 1. Ensure data directory exists ──────────────────────────────────────
if not exist "data" mkdir data

REM ── 2. Remove any pre-existing database ──────────────────────────────────
if exist "data\inventory.db" del /f /q "data\inventory.db"

echo =^> Creating SQLite database at data\inventory.db ...

REM ── 3. Write SQL to a temporary file then execute ─────────────────────────
REM     (sqlite3 on Windows does not reliably accept heredoc via pipe in CMD)

set "SQL_FILE=%TEMP%\setup_inventory_%RANDOM%.sql"

(
echo CREATE TABLE products ^(
echo     product_id   INTEGER PRIMARY KEY,
echo     name         TEXT    NOT NULL,
echo     category     TEXT    NOT NULL,
echo     unit_price   REAL    NOT NULL,
echo     stock_qty    INTEGER NOT NULL,
echo     last_updated TEXT    NOT NULL
echo ^);
echo.
echo INSERT INTO products VALUES
echo -- Electronics
echo ^( 1, 'Wireless Bluetooth Headphones',    'Electronics',  89.99,  142, '2024-11-15'^),
echo ^( 2, 'USB-C Hub 7-Port',                 'Electronics',  34.95,  305, '2024-12-01'^),
echo ^( 3, '4K Webcam 60fps',                  'Electronics', 129.00,   87, '2025-01-10'^),
echo ^( 4, 'Mechanical Keyboard TKL',          'Electronics',  74.50,  213, '2025-02-22'^),
echo ^( 5, 'Portable SSD 1TB',                'Electronics',  99.99,   66, '2025-03-05'^),
echo ^( 6, 'Smart LED Desk Lamp',              'Electronics',  45.00,  410, '2025-03-18'^),
echo ^( 7, 'Noise-Cancelling Earbuds',         'Electronics', 149.95,   55, '2025-04-01'^),
echo ^( 8, 'Wireless Charging Pad 15W',        'Electronics',  22.99,  527, '2025-04-14'^),
echo ^( 9, 'Ergonomic Mouse Vertical',         'Electronics',  58.00,  178, '2025-04-29'^),
echo ^(10, 'Mini Projector 1080p',             'Electronics', 219.00,   31, '2025-05-07'^),
echo -- Furniture
echo ^(11, 'Standing Desk 60 x 24 in',         'Furniture',   349.00,   18, '2024-10-03'^),
echo ^(12, 'Ergonomic Mesh Office Chair',      'Furniture',   289.99,   42, '2024-10-20'^),
echo ^(13, 'Bookcase 5-Shelf Walnut',          'Furniture',   159.00,   29, '2024-11-08'^),
echo ^(14, 'Monitor Arm Dual',                 'Furniture',    79.95,   94, '2024-12-12'^),
echo ^(15, 'Under-Desk Drawer Organizer',      'Furniture',    39.00,  201, '2025-01-05'^),
echo ^(16, 'Floating Wall Shelf Set',          'Furniture',    49.99,  155, '2025-01-28'^),
echo ^(17, 'Cable Management Tray',            'Furniture',    19.50,  437, '2025-02-10'^),
echo ^(18, 'Folding Laptop Stand Bamboo',      'Furniture',    29.95,  316, '2025-03-01'^),
echo ^(19, 'Corner Desk L-Shape 55 in',        'Furniture',   419.00,   12, '2025-03-22'^),
echo ^(20, 'Whiteboard Magnetic 36 x 24 in',   'Furniture',    69.00,   88, '2025-04-08'^),
echo -- Clothing
echo ^(21, 'Merino Wool Crew Neck Sweater',    'Clothing',     64.99,  243, '2024-09-15'^),
echo ^(22, 'Slim-Fit Chino Pants Navy',        'Clothing',     49.95,  317, '2024-09-28'^),
echo ^(23, 'Technical Running Jacket',         'Clothing',     89.00,  134, '2024-10-11'^),
echo ^(24, 'Insulated Puffer Vest',            'Clothing',     74.50,   98, '2024-10-25'^),
echo ^(25, 'Moisture-Wicking Polo Shirt',      'Clothing',     34.00,  502, '2024-11-02'^),
echo ^(26, 'Stretch Denim Jeans Dark Wash',    'Clothing',     59.95,  276, '2024-11-20'^),
echo ^(27, 'Quarter-Zip Fleece Pullover',      'Clothing',     54.99,  189, '2025-01-14'^),
echo ^(28, 'Lightweight Linen Blazer',         'Clothing',    119.00,   71, '2025-02-03'^),
echo ^(29, 'Athletic Compression Shorts',      'Clothing',     28.50,  445, '2025-02-18'^),
echo ^(30, 'Waterproof Trail Sneakers',        'Clothing',     99.00,  113, '2025-03-10'^),
echo -- Sports
echo ^(31, 'Adjustable Dumbbell Set 5-52 lb',  'Sports',      299.00,   24, '2024-08-22'^),
echo ^(32, 'Yoga Mat Non-Slip 6mm',            'Sports',       29.99,  618, '2024-09-05'^),
echo ^(33, 'Resistance Band Set 5-Pack',       'Sports',       24.95,  441, '2024-09-19'^),
echo ^(34, 'Foam Roller High-Density 18 in',   'Sports',       22.00,  384, '2024-10-07'^),
echo ^(35, 'Kettlebell Cast Iron 35 lb',       'Sports',       64.99,   77, '2024-10-30'^),
echo ^(36, 'Jump Rope Speed Cable',            'Sports',       18.50,  532, '2024-11-12'^),
echo ^(37, 'Pull-Up Bar Doorframe',            'Sports',       34.00,  209, '2024-12-05'^),
echo ^(38, 'Exercise Bike Magnetic Resistance', 'Sports',      449.00,    9, '2025-01-20'^),
echo ^(39, 'Whey Protein Powder Vanilla 2 lb', 'Sports',       39.95,  167, '2025-02-28'^),
echo ^(40, 'Running Hydration Vest 8L',        'Sports',       79.00,   54, '2025-03-15'^),
echo -- Kitchen
echo ^(41, 'Pour-Over Coffee Dripper Glass',   'Kitchen',      32.00,  293, '2024-07-14'^),
echo ^(42, 'Digital Kitchen Scale 11 lb',      'Kitchen',      19.95,  478, '2024-08-01'^),
echo ^(43, 'Stainless Chef Knife 8 in',        'Kitchen',      54.99,  222, '2024-08-19'^),
echo ^(44, 'Cast Iron Skillet 12 in',          'Kitchen',      44.00,  318, '2024-09-02'^),
echo ^(45, 'Bamboo Cutting Board Large',       'Kitchen',      27.50,  401, '2024-09-16'^),
echo ^(46, 'Instant-Read Thermometer',         'Kitchen',      22.00,  356, '2024-10-04'^),
echo ^(47, 'Silicone Utensil Set 6-Piece',     'Kitchen',      18.99,  604, '2024-10-18'^),
echo ^(48, 'French Press Borosilicate 34 oz',  'Kitchen',      36.95,  177, '2024-11-01'^),
echo ^(49, 'Vacuum Insulated Tumbler 20 oz',   'Kitchen',      29.99,  529, '2024-11-25'^),
echo ^(50, 'Spice Rack Organiser 20-Jar',      'Kitchen',      42.00,  214, '2025-01-09'^);
) > "%SQL_FILE%"

sqlite3 "data\inventory.db" < "%SQL_FILE%"
del /f /q "%SQL_FILE%"

echo.
echo =^> Database created successfully.
echo.

REM ── 4. Quick verification ─────────────────────────────────────────────────
for /f %%C in ('sqlite3 "data\inventory.db" "SELECT COUNT(*) FROM products;"') do set ROW_COUNT=%%C
echo     Table  : products
echo     Rows   : %ROW_COUNT%
echo     File   : data\inventory.db
echo.
echo     Category breakdown:
sqlite3 -column -header "data\inventory.db" "SELECT category, COUNT(*) AS rows, MIN(product_id) AS first_id, MAX(product_id) AS last_id FROM products GROUP BY category ORDER BY category;"

echo.
echo =^> Setup complete. Run run.bat to execute the pipeline.
endlocal
