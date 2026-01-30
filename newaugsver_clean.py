import pandas as pd
import random
import json
import argparse
from faker import Faker
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import os
import math

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable

field_constraints = {
    'RecordOperation': {'min_length': 1, 'max_length': 1},
    'OrganizationCode': {'min_length': 1, 'max_length': 1},
    'OrganizationTINType': {'min_length': 3, 'max_length': 3},
    'ProfitNonprofit': {'min_length': 1, 'max_length': 2},
    'OrganizationNPI': {'min_length': 10, 'max_length': 10},
    'PaymentMode': {'min_length': 3, 'max_length': 3},
    'RoutingTransitNumber': {'min_length': 9, 'max_length': 9},
    'AccountNumber': {'min_length': 1, 'max_length': 17},
    'AccountType': {'min_length': 6, 'max_length': 6},
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
    'OrganizationTIN': {'min_length': 9, 'max_length': 9},
    'PayeeID': {'min_length': 2, 'max_length': 9},  # was 7 -> now 9
    'OrganizationIdentifier': {'min_length': 3, 'max_length': 12}
}

# -------------------------------------------------------------
# PayeeID configuration
# -------------------------------------------------------------
# Numeric portion (after any prefix like MFR / DISP / PC) will be generated
# with a length between PAYEE_ID_MIN_DIGITS and PAYEE_ID_MAX_DIGITS inclusive.
# Increasing PAYEE_ID_MAX_DIGITS allows larger numeric segments (e.g. up to 9).
PAYEE_ID_MIN_DIGITS = 2
PAYEE_ID_MAX_DIGITS = 9  # Updated per new requirement (was implicitly smaller before)


class BankDataGenerator:
    def __init__(self, seed=None, as_of_date=None, blank_as_null: bool = False):
        self.fake = Faker()
        
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)
        else:
            Faker.seed()
        
        # Set as-of date for dynamic date generation
        if as_of_date:
            self.as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d")
        else:
            self.as_of_date = datetime.now()
        
        self.field_constraints = field_constraints
        
        # Load MFR organizations mapping from CSV
        mfr_csv_path = os.path.join(os.path.dirname(__file__), 'mfr_organizations.csv')
        try:
            self.mfr_orgs = pd.read_csv(mfr_csv_path).to_dict(orient='records')
        except FileNotFoundError:
            print(f"Warning: {mfr_csv_path} not found. Creating default MFR organizations.")
            self.mfr_orgs = [
                {'ID': 'MFR001', 'Name': 'Pfizer Inc.'},
                {'ID': 'MFR002', 'Name': 'Johnson & Johnson'},
                {'ID': 'MFR003', 'Name': 'Merck & Co., Inc.'}
            ]
        
        # Validate MFR org IDs don't exceed PayeeID max_length (for M, D, P: PayeeID = OrganizationIdentifier)
        payee_id_max = field_constraints.get('PayeeID', {}).get('max_length', 9)
        for mfr in self.mfr_orgs:
            if len(mfr['ID']) > payee_id_max:
                mfr['ID'] = mfr['ID'][:payee_id_max]
                print(f"Warning: MFR ID truncated to {payee_id_max} chars: {mfr['ID']}")
        
        self.company_names = [
            "Pfizer Inc.", "Johnson & Johnson", "Merck & Co., Inc.", "AbbVie Inc.", "Amgen Inc.",
            "Gilead Sciences, Inc.", "Biogen Inc.", "Regeneron Pharmaceuticals", "Moderna, Inc.",
            "Genentech", "Roche Holding AG", "Novartis AG", "GlaxoSmithKline (GSK)", "Sanofi S.A.",
            "AstraZeneca PLC", "Vertex Pharmaceuticals", "Alnylam Pharmaceuticals", "Bluebird Bio",
            "Sarepta Therapeutics", "CRISPR Therapeutics", "Beam Therapeutics"
        ]
        
        self.used_ids = set()
        self.used_r_identifiers = set()
        self.used_payee_ids = {'M': set(), 'D': set(), 'P': set()}
        self.payeeid_to_orgname = {}
        self.blank_as_null = blank_as_null  # new option to output nulls instead of empty strings
        
        # Field generators
        self.field_generators = {
            'RecordOperation': lambda: random.choice(['A', 'D']),
            'OrganizationCode': lambda: random.choice(['M', 'D', 'P', 'R']),
            'ProfitNonprofit': lambda: random.choice(['P', 'NP']),
            'OrganizationNPI': lambda: self.generate_npi(),
            'PaymentMode': lambda: random.choice(['EFT', 'CHK']),
            'RoutingTransitNumber': lambda: self.fake.numerify('#########'),
            'AccountNumber': lambda: self.fake.numerify('######'),
            'AccountType': lambda: random.choice(['CHKING', 'SAVING']),
            # Note: EffectiveStartDate and EffectiveEndDate are generated in generate_row() method
            'AddressCode': lambda: random.choice(['PMT', 'COR']),
            'AddressLine1': lambda: self.fake.street_address(),
            'AddressLine2': lambda: self.fake.street_address(),
            'CityName': lambda: self.fake.city(),
            'State': lambda: self.fake.state_abbr(),
            'PostalCode': lambda: self.fake.postcode(),
            'ContactCode': lambda: random.choice(['AO', 'DO']),
            'ContactFirstName': lambda: self.fake.first_name(),
            'ContactLastName': lambda: self.fake.last_name(),
            'ContactTitle': lambda: self.fake.job()[:23],
            'ContactPhone': lambda: self.fake.phone_number(),            'ContactFax': lambda: self.fake.phone_number(),
            'ContactOtherPhone': lambda: self.fake.phone_number(),
            'ContactEmail': lambda: self.fake.email()
        }

    def generate_start_date(self):
        """Generate EffectiveStartDate according to business rules:
        - Required and not null for all org codes (M, D, P, R)
        - If blank/prior date, system defaults to current date
        - If future date, process for prenote
        """
        # Generate mostly current date with minimal future for prenote testing
        rand = random.random()
        if rand < 0.95:  # 95% current date (most common, safest)
            return self.as_of_date.strftime('%Y-%m-%d')
        else:  # 5% future date (for prenote testing, very conservative)
            future_days = random.randint(1, 7)  # 1-7 days in future for prenote
            future_date = self.as_of_date + timedelta(days=future_days)
            return future_date.strftime('%Y-%m-%d')
    
    def generate_end_date(self, record_operation):
        """Generate EffectiveEndDate according to business rules:
        - Data Type: Date (YYYY-MM-DD format, exactly 10 chars when present)
        - Min Occurrence: 0 (can be blank/empty) 
        - Max Occurrence: 1 (single value when present)
        - Required: NO (optional field)
        - Business Rule: All D (Deactivated) records should have an EffectiveEndDate
        - If deactivated without end date, system uses current date
        - Can accept future dates
        """
        if record_operation == 'D':
            # Deactivated records: should have end date (per business rule)
            rand = random.random()
            if rand < 0.1:  # 10% blank (system will default to current date)
                return ''
            elif rand < 0.6:  # 50% current date (explicit deactivation)
                return self.as_of_date.strftime('%Y-%m-%d')
            else:  # 40% future date (scheduled deactivation)
                future_days = random.randint(1, 90)  # 1-90 days in future
                future_date = self.as_of_date + timedelta(days=future_days)
                return future_date.strftime('%Y-%m-%d')
        else:  # Active records (RecordOperation=A)
            # Active records: mostly blank end date (normal state)
            if random.random() < 0.85:  # 85% blank (normal for active)
                return ''
            else:  # 15% have future end date (scheduled deactivation)
                future_days = random.randint(30, 365)  # 30-365 days in future
                future_date = self.as_of_date + timedelta(days=future_days)
                return future_date.strftime('%Y-%m-%d')
    
    def validate_field(self, field_name, value):
        if field_name not in self.field_constraints:
            return value
            
        constraints = self.field_constraints[field_name]
        
        # Special handling for fields that can be blank (Min Occurrence = 0)
        if field_name in ['RoutingTransitNumber', 'AccountNumber', 'AccountType', 'OrganizationTIN', 'EffectiveEndDate'] and value == '':
            return ''
        
        if field_name == 'OrganizationTINType':
            # Allow only EIN, SSN or blank (blank only valid for R records generated earlier)
            if value not in ('EIN', 'SSN', ''):
                value = 'EIN'
        
        if isinstance(value, str):
            if 'max_length' in constraints and len(value) > constraints['max_length']:
                value = value[:constraints['max_length']]
                
            if 'min_length' in constraints and len(value) < constraints['min_length'] and value != '':
                if field_name in ['OrganizationNPI', 'RoutingTransitNumber', 'AccountNumber', 'OrganizationTIN']:
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
    
    def generate_unique_r_identifier(self):
        """Generate a unique 10-digit identifier for R records"""
        while True:
            first_digit = str(random.randint(1, 9))
            nine_digits = ''.join(random.choice('0123456789') for _ in range(9))
            identifier = first_digit + nine_digits
            
            if identifier not in self.used_r_identifiers:
                self.used_r_identifiers.add(identifier)
                return identifier
    
    def generate_npi(self):
        """Generate a 10-digit NPI with a non-zero first digit."""
        first = str(random.randint(1, 9))
        rest = ''.join(random.choice('0123456789') for _ in range(9))
        return first + rest
    
    def generate_unique_id(self, org_code, prefix, num):
        """Generate a unique ID for the given organization code.
        
        For M, D, P codes: PayeeID must equal OrganizationIdentifier.
        PayeeID max_length is 9, so we truncate the numeric portion to fit.
        """
        if org_code in ['M', 'D', 'P']:
            # Calculate max digits for numeric portion to fit within PayeeID max_length (9)
            payee_id_max = self.field_constraints.get('PayeeID', {}).get('max_length', 9)
            max_num_digits = payee_id_max - len(prefix)
            
            # Truncate num to fit within the allowed digits
            num_str = str(num)
            if len(num_str) > max_num_digits:
                num_str = num_str[:max_num_digits]
            
            base_id = f"{prefix}{num_str}"
            counter = 0
            while base_id in self.used_payee_ids[org_code]:
                counter += 1
                # Regenerate with counter, ensuring it still fits
                new_num = int(num_str) + counter
                new_num_str = str(new_num)
                if len(new_num_str) > max_num_digits:
                    new_num_str = new_num_str[:max_num_digits]
                base_id = f"{prefix}{new_num_str}"
            self.used_payee_ids[org_code].add(base_id)
            return base_id
        else:
            # For R - PayeeID can be duplicated
            return f"{prefix}{num}"

    def generate_row(self):
        # OrganizationCode: strictly enforce allowed values and length
        code = random.choice(['M', 'D', 'P', 'R'])
        # Widen numeric space for potential longer IDs (PayeeID up to 9 digits)
        # Safety: ensure configured bounds are sensible
        min_digits = max(1, PAYEE_ID_MIN_DIGITS)
        max_digits = max(min_digits, PAYEE_ID_MAX_DIGITS)
        # Generate a random integer with digit length between min_digits and max_digits
        target_digits = random.randint(min_digits, max_digits)
        low = 10 ** (target_digits - 1)
        high = (10 ** target_digits) - 1
        num = random.randint(low, high)

        # Generate organization identifier and name based on code
        if code == 'M':
            # For M records, select from available MFR orgs
            available_mfr = [mfr for mfr in self.mfr_orgs if mfr['ID'] not in self.used_payee_ids['M']]
            if available_mfr:
                mfr_entry = random.choice(available_mfr)
                org_identifier = mfr_entry['ID']
                org_name = mfr_entry['Name']
                self.used_payee_ids['M'].add(org_identifier)
            else:
                org_identifier = self.generate_unique_id('M', 'MFR', num)
                org_name = random.choice(self.company_names)
        elif code == 'R':
            org_identifier = self.generate_unique_r_identifier()
            org_name = random.choice(self.company_names)
        elif code == 'D':
            org_identifier = self.generate_unique_id('D', 'DISP', num)
            org_name = random.choice(self.company_names)
        elif code == 'P':
            org_identifier = self.generate_unique_id('P', 'PC', num)
            org_name = random.choice(self.company_names)

        # PayeeID business rules
        if code in ['D', 'P', 'M']:
            payee_id = org_identifier
        elif code == 'R':
            payee_id = f"DISP{num}"
            if payee_id == org_identifier:
                payee_id = payee_id + str(random.randint(0,9))

        # PaymentMode logic
        if code == 'M':
            payment_mode = 'EFT'  # Force EFT for M records
        else:
            payment_mode = random.choice(['EFT', 'CHK'])

        # Banking details based on PaymentMode
        if payment_mode == 'EFT':
            routing = self.fake.numerify('#########')
            account = self.fake.numerify('######')
            account_type = random.choice(['CHKING', 'SAVING'])
        else:
            routing = ''
            account = ''
            account_type = ''

        # For R records, clear banking/address fields
        if code == 'R':
            routing = ''
            account = ''
            account_type = ''
            address_code = ''
            address_line1 = ''
            address_line2 = ''
            city_name = ''
            state = ''
            postal_code = ''
            contact_first_name = ''
            contact_last_name = ''
        else:
            # Generate address fields
            if code == 'M':
                address_code = random.choice(['COR', ''])
            elif code in ['D', 'P']:
                address_code = 'PMT'
            else:
                address_code = random.choice(['PMT', 'COR']) if payment_mode == 'CHK' or random.choice([True, False]) else ''
            
            address_line1 = self.fake.street_address() if address_code else ''
            address_line2 = self.fake.secondary_address() if address_code and random.choice([True, False]) else ''
            city_name = self.fake.city() if address_code else ''
            state = self.fake.state_abbr() if address_code else ''
            postal_code = self.fake.postcode() if address_code else ''
            contact_first_name = self.fake.first_name()
            contact_last_name = self.fake.last_name()
        
        # Generate RecordOperation
        record_operation = random.choice(['A', 'D'])
        
        # Generate EffectiveStartDate and EffectiveEndDate
        effective_start_date = self.generate_start_date()  # Always required, never blank
        effective_end_date = self.generate_end_date(record_operation)  # Pass record_operation
          # Fix edge case: if both start and end dates are set, ensure end date >= start date
        if effective_end_date and effective_start_date:
            # If we have both dates, make sure end date is not before start date
            start_dt = datetime.strptime(effective_start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(effective_end_date, '%Y-%m-%d')
            if end_dt < start_dt:
                # Set end date to at least same day as start date, or a few days after
                end_dt = start_dt + timedelta(days=random.randint(0, 7))
                effective_end_date = end_dt.strftime('%Y-%m-%d')

        # OrganizationTIN logic
        if code == 'M':
            org_tin = self.fake.numerify('#########')
            if random.random() < 0.15:  # 15% chance for all 9's
                org_tin = '999999999'
            # Keep all generated TINs, including 999999999 (submit the 9's)
        elif code in ['D', 'P']:
            org_tin = self.fake.numerify('#########')  # Required
        else:  # R
            org_tin = ''

        # OrganizationTINType logic (Allowed: EIN, SSN; Required for M,D,P; R must be blank)
        if code == 'R':
            org_tin_type = ''
        else:
            org_tin_type = random.choice(['EIN', 'SSN'])

        # ProfitNonprofit logic
        if code in ['D', 'P']:
            profit_nonprofit = random.choice(['P', 'NP'])  # Required
        elif code == 'M':
            profit_nonprofit = random.choice(['', 'P', 'NP'])  # Optional
        else:  # R
            profit_nonprofit = ''

        # OrganizationNPI logic
        if random.random() < 0.2:  # 20% chance to be empty
            org_npi = ''
        else:
            org_npi = self.generate_npi()

        # OrganizationLegalName logic
        if org_tin_type == 'EIN':
            org_legal_name = org_name
        else:  # SSN
            first = self.fake.first_name()
            last = self.fake.last_name()
            org_legal_name = f"{first} {last}"[:40]

        # ContactTitle logic
        if code not in ['M', 'R'] and random.random() < 0.7:
            contact_title = self.fake.job()[:23]
        else:
            contact_title = ''
        
        # Build the row
        row = {
            'RecordOperation': record_operation,
            'OrganizationCode': code,
            'PayeeID': payee_id,
            'OrganizationIdentifier': org_identifier,
            'OrganizationName': org_name[:40],
            'OrganizationLegalName': org_legal_name,
            'OrganizationTIN': org_tin,
            'OrganizationTINType': org_tin_type,
            'ProfitNonprofit': profit_nonprofit,
            'OrganizationNPI': org_npi,
            'PaymentMode': payment_mode,
            'RoutingTransitNumber': routing,
            'AccountNumber': account,
            'AccountType': account_type,
            'EffectiveStartDate': effective_start_date,
            'EffectiveEndDate': effective_end_date,
            'AddressCode': address_code,
            'AddressLine1': address_line1,
            'AddressLine2': address_line2,
            'CityName': city_name,
            'State': state,
            'PostalCode': postal_code,
            'ContactCode': random.choice(['AO', 'DO']),
            'ContactFirstName': contact_first_name,
            'ContactLastName': contact_last_name,
            'ContactTitle': contact_title,
            'ContactPhone': self.fake.phone_number(),
            'ContactFax': self.fake.phone_number() if random.choice([True, False]) else '',
            'ContactOtherPhone': self.fake.phone_number() if random.choice([True, False]) else '',
            'ContactEmail': self.fake.email()
        }
        # Validate fields
        for field_name, value in row.items():
            row[field_name] = self.validate_field(field_name, value)
        # If option enabled, convert empty strings to None (null) AFTER validation trimming/padding
        if self.blank_as_null:
            for k, v in list(row.items()):
                if v == '':
                    row[k] = None
        return row

    def generate_data(self, num_rows):
        """Generate specified number of rows"""
        data = []
        for _ in tqdm(range(num_rows), desc="Generating rows"):
            data.append(self.generate_row())
        df = pd.DataFrame(data)
        # Convert date columns to actual date dtype (date32 in Parquet)
        for col in ['EffectiveStartDate', 'EffectiveEndDate']:
            if col in df.columns:
                # Blank -> NA then to datetime
                df[col] = pd.to_datetime(df[col].replace('', pd.NA), errors='coerce').dt.date
        return df

def write_parquet_stream(generator, total_rows, filepath, batch_size=50000):
    """Stream-generate rows and write to Parquet in batches to avoid high memory usage."""
    from pyarrow.parquet import ParquetWriter
    written = 0
    writer = None
    try:
        while written < total_rows:
            to_make = min(batch_size, total_rows - written)
            batch_rows = [generator.generate_row() for _ in range(to_make)]
            batch_df = pd.DataFrame(batch_rows)
            # Date coercion (reuse logic)
            for col in ['EffectiveStartDate', 'EffectiveEndDate']:
                if col in batch_df.columns:
                    batch_df[col] = pd.to_datetime(batch_df[col].replace('', pd.NA), errors='coerce').dt.date
            table = pa.Table.from_pandas(batch_df, preserve_index=False)
            if writer is None:
                writer = ParquetWriter(filepath, table.schema)
            writer.write_table(table)
            written += to_make
            if written % (batch_size * 2) == 0 or written == total_rows:
                print(f"... {written}/{total_rows} rows written")
    finally:
        if writer is not None:
            writer.close()
    print(f"✅ Streaming Parquet complete: {filepath} ({total_rows} rows)")

def save_to_formats(df, output_dir, filename_base, formats, extra_columns=None, null_display: str = ''):
    """Save DataFrame to specified formats.
    null_display: token used to represent nulls in flat outputs (CSV/XLSX). Parquet keeps true nulls.
    """
    # Add extra columns if specified
    if extra_columns:
        for col in extra_columns:
            df[col] = f"Extra_{col}_" + df.index.astype(str)
    saved_files = []
    # Work list of date columns
    date_cols = [c for c in ['EffectiveStartDate', 'EffectiveEndDate'] if c in df.columns]
    for fmt in formats:
        ffmt = fmt.lower()
        if ffmt == 'csv':
            tmp = df.copy()
            # Format date columns but keep nulls until after formatting
            for c in date_cols:
                tmp[c] = pd.to_datetime(tmp[c], errors='coerce').dt.strftime('%Y-%m-%d')
            if null_display:
                tmp = tmp.replace({pd.NA: None})  # normalize
                for c in tmp.columns:
                    # Replace real nulls
                    tmp[c] = tmp[c].where(~tmp[c].isna(), other=null_display)
                    # Also replace empty strings so user sees token
                    tmp[c] = tmp[c].replace({'': null_display})
                for c in date_cols:
                    tmp[c] = tmp[c].replace('NaT', null_display)
                na_rep_val = null_display
            else:
                for c in date_cols:
                    tmp[c] = tmp[c].replace('NaT', '')
                na_rep_val = ''
            filepath = os.path.join(output_dir, f"{filename_base}.csv")
            tmp.to_csv(filepath, index=False, na_rep=na_rep_val)
            saved_files.append(filepath)
            print(f"✅ Saved CSV: {filepath}")
        elif ffmt == 'parquet':
            filepath = os.path.join(output_dir, f"{filename_base}.parquet")
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, filepath)
            saved_files.append(filepath)
            print(f"✅ Saved Parquet: {filepath}")
        elif ffmt == 'xlsx':
            tmp = df.copy()
            for c in date_cols:
                tmp[c] = pd.to_datetime(tmp[c], errors='coerce').dt.strftime('%Y-%m-%d')
            if null_display:
                for c in tmp.columns:
                    tmp[c] = tmp[c].where(~tmp[c].isna(), other=null_display)
                    tmp[c] = tmp[c].replace({'': null_display})
                for c in date_cols:
                    tmp[c] = tmp[c].replace('NaT', null_display)
                na_rep_val = null_display
            else:
                for c in date_cols:
                    tmp[c] = tmp[c].replace('NaT', '')
                na_rep_val = ''
            filepath = os.path.join(output_dir, f"{filename_base}.xlsx")
            tmp.to_excel(filepath, index=False, engine='openpyxl', na_rep=na_rep_val)
            saved_files.append(filepath)
            print(f"✅ Saved Excel: {filepath}")
        elif ffmt == 'json':
            tmp = df.copy()
            date_cols = [c for c in ['EffectiveStartDate', 'EffectiveEndDate'] if c in tmp.columns]
            for c in date_cols:
                tmp[c] = pd.to_datetime(tmp[c], errors='coerce').dt.strftime('%Y-%m-%d')
                tmp[c] = tmp[c].replace('NaT', None)
            filepath = os.path.join(output_dir, f"{filename_base}.json")
            tmp.to_json(filepath, orient='records', indent=2)
            saved_files.append(filepath)
            print(f"✅ Saved JSON: {filepath}")
    return saved_files

def main():
    parser = argparse.ArgumentParser(description="Bank Data Generator")
    parser.add_argument("--rows", type=int, default=50, help="Number of rows to generate")
    parser.add_argument("--seed", type=int, help="Random seed for reproducible output")
    parser.add_argument("--as-of-date", type=str, help="As-of date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--formats", nargs="+", default=["parquet"], help="Output formats: csv, parquet, xlsx, json")
    parser.add_argument("--output-dir", type=str, default="./output", help="Output directory")
    parser.add_argument("--output", type=str, default="bank_data", help="Output filename base")
    parser.add_argument("--extra-columns", nargs="*", help="Add extra columns with specified names")
    parser.add_argument("--invalid-extension", type=str, help="Use invalid file extension")
    parser.add_argument("--stream-batch-size", type=int, help="Optional batch size for streaming large Parquet output")
    parser.add_argument("--blank-as-null", action="store_true", help="Convert blank optional fields to actual null values")
    parser.add_argument("--null-display", type=str, default="", help="Token to display for nulls in CSV/XLSX outputs (e.g. NULL). Empty default leaves cells blank")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Initialize generator
    generator = BankDataGenerator(seed=args.seed, as_of_date=args.as_of_date, blank_as_null=args.blank_as_null)
    
    large_threshold = 300000  # switch to streaming above this
    do_stream = args.stream_batch_size is not None or (args.rows > large_threshold and 'parquet' in [f.lower() for f in args.formats])

    if do_stream and set([f.lower() for f in args.formats]) == {'parquet'}:
        # Only Parquet requested and streaming enabled/needed
        os.makedirs(args.output_dir, exist_ok=True)
        out_path = os.path.join(args.output_dir, f"{args.output}.parquet")
        batch_size = args.stream_batch_size or 50000
        print(f"Streaming generation: {args.rows} rows -> {out_path} (batch {batch_size})")
        write_parquet_stream(generator, args.rows, out_path, batch_size=batch_size)
        print(f"🎉 Generated {args.rows} rows successfully (stream mode)!")
        return

    # Fallback normal in-memory path
    print(f"Generating {args.rows} rows with seed {args.seed}...")
    df = generator.generate_data(args.rows)
    
    # Handle invalid extension
    if args.invalid_extension:
        formats = [args.invalid_extension]
        filename_base = args.output
        filepath = os.path.join(args.output_dir, f"{filename_base}.{args.invalid_extension}")
        df.to_csv(filepath, index=False)
        print(f"✅ Saved file with invalid extension: {filepath}")
    else:
        # Save in requested formats
        save_to_formats(df, args.output_dir, args.output, args.formats, args.extra_columns, null_display=args.null_display)
    
    print(f"🎉 Generated {len(df)} rows successfully!")

if __name__ == "__main__":
    main()
