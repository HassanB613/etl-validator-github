import pandas as pd
import random
import json
import argparse
from faker import Faker
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import os

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable

field_constraints = {
    'RecordOperation': {'min_length': 1, 'max_length': 1},
    'OrganizationCode': {'min_length': 1, 'max_length': 1},
    'OrganizationTinType': {'min_length': 3, 'max_length': 3},
    'ProfitNonprofit': {'min_length': 1, 'max_length': 2},
    'OrganizationNPI': {'min_length': 10, 'max_length': 10},
    'PaymentMode': {'min_length': 3, 'max_length': 3},
    'RoutingTransitNumber': {'min_length': 9, 'max_length': 9},
    'AccountNumber': {'min_length': 1, 'max_length': 17},
    'AccountType': {'min_length': 3, 'max_length': 3},
    'EffectiveStartDate': {'min_length': 10, 'max_length': 10},
    'EffectiveEndDate': {'min_length': 10, 'max_length': 10},
    'AddressCode': {'min_length': 1, 'max_length': 10},
    'AddressLine1': {'min_length': 1, 'max_length': 40},
    'AddressLine2': {'min_length': 1, 'max_length': 40},
    'CityName': {'min_length': 1, 'max_length': 25},
    'State': {'min_length': 2, 'max_length': 2},
    'PostalCode': {'min_length': 5, 'max_length': 10},
    'ContactCode': {'min_length': 1, 'max_length': 2},
    'ContactFirstName': {'min_length': 1, 'max_length': 20},
    'ContactLastName': {'min_length': 1, 'max_length': 25},
    'ContactTitle': {'min_length': 1, 'max_length': 23},
    'ContactPhone': {'min_length': 1, 'max_length': 25},
    'ContactFax': {'min_length': 1, 'max_length': 25},
    'ContactOtherPhone': {'min_length': 1, 'max_length': 25},
    'ContactEmail': {'min_length': 1, 'max_length': 99},
    'OrganizationName': {'min_length': 1, 'max_length': 40},
    'OrganizationLegalName': {'min_length': 1, 'max_length': 40},
    'OrganizationTin': {'min_length': 9, 'max_length': 9},
    'PayeeID': {'min_length': 4, 'max_length': 4},
    'OrganizationIdentifier': {'min_length': 4, 'max_length': 4}
}

class BankDataGenerator:
    def __init__(self, seed=None):
        self.fake = Faker()
        
        if seed is not None:
            random.seed(seed)
            self.fake.seed_instance(seed)
        else:
            self.fake.seed_instance(random.randint(1, 10000))
        
        self.field_constraints = field_constraints
        
        self.company_names = [
            "Pfizer Inc.", "Johnson & Johnson", "Merck & Co., Inc.", "AbbVie Inc.", "Amgen Inc.",
            "Gilead Sciences, Inc.", "Biogen Inc.", "Regeneron Pharmaceuticals", "Moderna, Inc.",
            "Genentech", "Roche Holding AG", "Novartis AG", "GlaxoSmithKline (GSK)", "Sanofi S.A.",
            "AstraZeneca PLC", "Vertex Pharmaceuticals", "Alnylam Pharmaceuticals", "Bluebird Bio",
            "Sarepta Therapeutics", "CRISPR Therapeutics", "Beam Therapeutics"
        ]
        
        self.used_ids = set()
        
        self.field_generators = {
            'RecordOperation': lambda: random.choice(['A', 'D']),
            'OrganizationCode': lambda: random.choice(['M', 'D', 'P']),
            'OrganizationTinType': lambda: random.choice(['EIN', 'SSN']),
            'ProfitNonprofit': lambda: random.choice(['P', 'NP']),
            'OrganizationNPI': lambda: self.fake.numerify('##########'),
            'PaymentMode': lambda: random.choice(['EFT', 'CHK']),
            'RoutingTransitNumber': lambda: self.fake.numerify('#########'),
            'AccountNumber': lambda: self.fake.numerify('######'),
            'AccountType': lambda: random.choice(['CHK', 'EFT']),
            'EffectiveStartDate': lambda: self.fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
            'EffectiveEndDate': lambda: self.fake.date_between(start_date='today', end_date='+1y').strftime('%Y-%m-%d'),
            'AddressCode': lambda: random.choice(['PMT', 'COR', 'LGL', 'BILL']),
            'AddressLine1': lambda: self.fake.street_address(),
            'AddressLine2': lambda: self.fake.street_address(),
            'CityName': lambda: self.fake.city(),
            'State': lambda: self.fake.state_abbr(),
            'PostalCode': lambda: self.fake.postcode(),
            'ContactCode': lambda: random.choice(['AO', 'DO']),
            'ContactFirstName': lambda: self.fake.first_name(),
            'ContactLastName': lambda: self.fake.last_name(),
            'ContactTitle': lambda: self.fake.job()[:23],
            'ContactPhone': lambda: self.fake.phone_number(),
            'ContactFax': lambda: self.fake.phone_number(),
            'ContactOtherPhone': lambda: self.fake.phone_number(),
            'ContactEmail': lambda: self.fake.email()
        }
    
    def validate_field(self, field_name, value):
        if field_name not in self.field_constraints:
            return value
            
        constraints = self.field_constraints[field_name]
        
        if isinstance(value, str):
            if 'max_length' in constraints and len(value) > constraints['max_length']:
                value = value[:constraints['max_length']]
                
            if 'min_length' in constraints and len(value) < constraints['min_length']:
                if field_name in ['OrganizationNPI', 'RoutingTransitNumber', 'AccountNumber', 'OrganizationTin']:
                    value = value.zfill(constraints['min_length'])
                else:
                    value = value.ljust(constraints['min_length'])
                
        return value
    
    def generate_unique_m_id(self):
        while True:
            val = f"M{random.randint(0, 999):03d}"
            
            if val not in self.used_ids:
                self.used_ids.add(val)
                return val
    
    def generate_row(self):
        m_id = self.generate_unique_m_id()
        
        row = {
            'PayeeID': m_id,
            'OrganizationIdentifier': m_id,
        }
        
        row['OrganizationCode'] = self.field_generators['OrganizationCode']()
        
        tin_type = self.field_generators['OrganizationTinType']()
        row['OrganizationTinType'] = tin_type
        
        if row['OrganizationCode'] == 'M' and random.random() < 0.15:
            row['OrganizationTin'] = '999999999'
        else:
            row['OrganizationTin'] = self.fake.numerify('#########')
        
        name = random.choice(self.company_names) if tin_type == 'EIN' else self.fake.name()
        row['OrganizationName'] = name
        row['OrganizationLegalName'] = name
        
        for field, generator in self.field_generators.items():
            if field not in row:
                row[field] = generator()
        
        for field in row:
            row[field] = self.validate_field(field, row[field])
                
        return row
    
    def generate_dataset(self, num_rows):
        rows = [self.generate_row() for _ in tqdm(range(num_rows), desc="Generating data")]
        return pd.DataFrame(rows)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Generate synthetic bank data")
    parser.add_argument('--rows', type=int, default=50, 
                        help='Number of rows to generate')
    parser.add_argument('--seed', type=int, 
                        help='Random seed for reproducible data')
    parser.add_argument('--output', default='', 
                        help='Base filename for output files (default: mtfdm.dev.DMBankData.<timestamp>)')
    parser.add_argument('--output-dir', default='.', 
                        help='Directory to save output files')
    parser.add_argument('--formats', nargs='+', 
                        choices=['csv', 'xlsx', 'parquet', 'json'],
                        default=['csv', 'xlsx', 'parquet'], 
                        help='Output file formats')
    parser.add_argument('--missing-data', action='store_true', 
                        help='Generate dataset with missing values')
    parser.add_argument('--missing-columns', nargs='+', 
                        help='Specific columns to have missing data')
    parser.add_argument('--missing-percent', type=float, default=20.0, 
                        help='Percentage of rows to have missing data (0-100)')
    parser.add_argument('--missing-pattern', 
                        choices=['random', 'consecutive', 'first', 'last'],
                        default='random', 
                        help='Pattern for missing data distribution')
    args = parser.parse_args()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    logger.info(f"Initializing generator with seed: {args.seed}")
    generator = BankDataGenerator(seed=args.seed)
    df = generator.generate_dataset(args.rows)
    
    if args.missing_data:
        columns_to_affect = args.missing_columns or df.columns.tolist()
        missing_count = int(args.rows * args.missing_percent / 100)
        
        logger.info(f"Adding missing data to {len(columns_to_affect)} columns ({args.missing_percent}% of rows)")
        
        missing_patterns = {
            'random': lambda: random.sample(range(args.rows), missing_count),
            'consecutive': lambda: range(
                random.randint(0, args.rows - missing_count), 
                random.randint(0, args.rows - missing_count) + missing_count
            ),
            'first': lambda: range(missing_count),
            'last': lambda: range(args.rows - missing_count, args.rows)
        }
        
        for column in columns_to_affect:
            if column in df.columns:
                rows_to_nullify = missing_patterns[args.missing_pattern]()
                df.loc[rows_to_nullify, column] = None
                logger.info(f"Added missing values to column '{column}'")
            else:
                logger.warning(f"Column '{column}' not found, skipping")
    
    timestamp = datetime.now().strftime('%Y%m%d.%H%M%S')
    file_prefix = f"mtfdm.dev.DMBankData.{timestamp}"
    
    base_filename = args.output if args.output else file_prefix
    
    save_functions = {
        'csv': lambda df, file: df.to_csv(file, index=False),
        'xlsx': lambda df, file: df.to_excel(file, index=False),
        'parquet': lambda df, file: pq.write_table(
            pa.Table.from_pandas(df).replace_schema_metadata({
                b'GeneratedAt': timestamp.encode('utf-8'),
                b'RowCount': str(len(df)).encode('utf-8'),
                b'GeneratorSeed': str(args.seed).encode('utf-8') if args.seed else b'None'
            }),
            file, 
            coerce_timestamps="ms"
        ),
        'json': lambda df, file: df.to_json(file, orient='records', lines=True)
    }
    
    for fmt in args.formats:
        filename = f"{args.output_dir}/{base_filename}.{fmt}"
        logger.info(f"Saving data to {filename}")
        
        try:
            save_functions[fmt](df, filename)
            logger.info(f"✅ Successfully saved {fmt.upper()} file")
        except Exception as e:
            logger.error(f"Failed to save {fmt.upper()} file: {str(e)}")
    
    logger.info(f"✅ Data generation complete! Generated {len(df)} rows with {len(df.columns)} columns")

if __name__ == "__main__":
    main()