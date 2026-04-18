"""
ML Anomaly Detection Model for Market Risk Data Validation
Uses Isolation Forest + rule-based ensemble to detect data quality issues.
Designed for MCRM Data Management & Maintenance (DMM) pipeline.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
import pickle
import os
import sqlite3


class MarketDataAnomalyDetector:
    """
    Ensemble anomaly detector combining:
    1. Isolation Forest (unsupervised ML) - catches novel/unknown patterns
    2. Rule-Based Engine (domain knowledge) - catches known data quality issues
    3. Statistical Thresholds (Z-score, IQR) - catches distributional outliers
    """

    def __init__(self, contamination=0.05):
        self.contamination = contamination
        self.isolation_forest = IsolationForest(
            contamination=contamination,
            n_estimators=200,
            max_samples='auto',
            random_state=42,
            n_jobs=-1
        )
        self.scaler = StandardScaler()
        self.is_fitted = False
        self.feature_columns = []
        self.training_stats = {}

    def _engineer_features(self, df):
        """Create features for anomaly detection from market data."""
        features = pd.DataFrame(index=df.index)

        # Price-based features
        features['mid_price'] = df['mid_price'].fillna(0)
        features['has_mid_price'] = (~df['mid_price'].isna()).astype(int)

        # Bid-Ask features
        features['bid_price'] = df['bid_price'].fillna(0)
        features['ask_price'] = df['ask_price'].fillna(0)
        features['has_quotes'] = (~df['bid_price'].isna() & ~df['ask_price'].isna()).astype(int)

        # Spread features
        features['spread'] = np.where(
            features['has_quotes'] == 1,
            features['ask_price'] - features['bid_price'],
            0
        )
        features['spread_pct'] = np.where(
            (features['has_quotes'] == 1) & (features['mid_price'] > 0),
            features['spread'] / features['mid_price'],
            0
        )

        # Market quality indicators
        features['is_crossed'] = (features['bid_price'] > features['ask_price']).astype(int)
        features['is_negative_price'] = (features['mid_price'] < 0).astype(int)
        features['volume'] = df['volume'].fillna(0)
        features['log_volume'] = np.log1p(features['volume'])
        features['is_zero_volume'] = (features['volume'] == 0).astype(int)

        # Staleness features
        features['is_stale'] = df['is_stale'].fillna(0).astype(int)

        # Price magnitude (log scale for normalization)
        features['log_price'] = np.where(
            features['mid_price'] > 0,
            np.log(features['mid_price']),
            0
        )

        self.feature_columns = features.columns.tolist()
        return features

    def fit(self, df):
        """Train the anomaly detection model on historical market data."""
        print("Engineering features...")
        features = self._engineer_features(df)

        # Store training statistics for rule-based checks
        clean_data = df[df['quality_flag'] == 'CLEAN'] if 'quality_flag' in df.columns else df
        clean_features = self._engineer_features(clean_data)

        self.training_stats = {
            'price_mean': clean_features['mid_price'].mean(),
            'price_std': clean_features['mid_price'].std(),
            'spread_pct_mean': clean_features['spread_pct'].mean(),
            'spread_pct_std': clean_features['spread_pct'].std(),
            'volume_mean': clean_features['volume'].mean(),
            'volume_std': clean_features['volume'].std(),
            'price_q1': clean_features['mid_price'].quantile(0.25),
            'price_q3': clean_features['mid_price'].quantile(0.75),
        }
        self.training_stats['price_iqr'] = (
            self.training_stats['price_q3'] - self.training_stats['price_q1']
        )

        print("Scaling features...")
        scaled_features = self.scaler.fit_transform(features)

        print("Training Isolation Forest...")
        self.isolation_forest.fit(scaled_features)
        self.is_fitted = True

        # Evaluate on training data if labels exist
        if 'quality_flag' in df.columns:
            predictions = self.predict(df)
            actual_anomalies = (df['quality_flag'] != 'CLEAN').astype(int)
            predicted_anomalies = (predictions['anomaly_score'] < 0).astype(int)

            tp = ((predicted_anomalies == 1) & (actual_anomalies == 1)).sum()
            fp = ((predicted_anomalies == 1) & (actual_anomalies == 0)).sum()
            fn = ((predicted_anomalies == 0) & (actual_anomalies == 1)).sum()

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

            print(f"\nModel Performance on Training Data:")
            print(f"  Precision: {precision:.3f}")
            print(f"  Recall:    {recall:.3f}")
            print(f"  F1-Score:  {f1:.3f}")
            print(f"  Anomalies detected: {predicted_anomalies.sum():,} / {actual_anomalies.sum():,} actual")

        return self

    def predict(self, df):
        """
        Run anomaly detection combining ML model + rule-based checks.
        Returns DataFrame with anomaly scores and classifications.
        """
        if not self.is_fitted:
            raise ValueError("Model not fitted. Call fit() first.")

        features = self._engineer_features(df)
        scaled_features = self.scaler.transform(features)

        # ML predictions
        ml_scores = self.isolation_forest.decision_function(scaled_features)
        ml_predictions = self.isolation_forest.predict(scaled_features)

        results = pd.DataFrame(index=df.index)
        results['instrument_id'] = df['instrument_id'].values
        results['date'] = df['date'].values
        results['mid_price'] = df['mid_price'].values
        results['anomaly_score'] = ml_scores
        results['ml_anomaly'] = (ml_predictions == -1).astype(int)

        # Rule-based checks
        results['rule_missing_quotes'] = (
            df['bid_price'].isna() | df['ask_price'].isna()
        ).astype(int)

        results['rule_negative_price'] = (
            df['mid_price'].fillna(0) < 0
        ).astype(int)

        results['rule_crossed_market'] = np.where(
            (~df['bid_price'].isna()) & (~df['ask_price'].isna()),
            (df['bid_price'] > df['ask_price']).astype(int),
            0
        )

        results['rule_stale'] = df['is_stale'].fillna(0).astype(int)

        results['rule_zero_volume'] = (df['volume'] == 0).astype(int)

        # Statistical checks
        if self.training_stats.get('price_std', 0) > 0:
            z_scores = np.abs(
                (df['mid_price'].fillna(0) - self.training_stats['price_mean'])
                / self.training_stats['price_std']
            )
            results['rule_price_zscore_flag'] = (z_scores > 3).astype(int)
        else:
            results['rule_price_zscore_flag'] = 0

        # Ensemble: combine ML + rules
        rule_columns = [c for c in results.columns if c.startswith('rule_')]
        results['rule_flag_count'] = results[rule_columns].sum(axis=1)
        results['ensemble_anomaly'] = (
            (results['ml_anomaly'] == 1) | (results['rule_flag_count'] > 0)
        ).astype(int)

        # Severity classification
        conditions = [
            (results['rule_negative_price'] == 1) | (results['rule_price_zscore_flag'] == 1),
            (results['rule_crossed_market'] == 1) | (results['rule_missing_quotes'] == 1),
            (results['rule_stale'] == 1) | (results['ml_anomaly'] == 1),
            (results['rule_zero_volume'] == 1),
        ]
        choices = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
        results['severity'] = np.select(conditions, choices, default='CLEAN')

        # Issue type classification
        issue_conditions = [
            results['rule_negative_price'] == 1,
            results['rule_price_zscore_flag'] == 1,
            results['rule_crossed_market'] == 1,
            results['rule_missing_quotes'] == 1,
            results['rule_stale'] == 1,
            results['rule_zero_volume'] == 1,
            results['ml_anomaly'] == 1,
        ]
        issue_choices = [
            'NEGATIVE_PRICE', 'PRICE_SPIKE', 'CROSSED_MARKET',
            'MISSING_QUOTES', 'STALE_PRICE', 'ZERO_VOLUME', 'ML_DETECTED'
        ]
        results['detected_issue'] = np.select(issue_conditions, issue_choices, default='CLEAN')

        return results

    def get_model_summary(self):
        """Return model configuration and training summary."""
        return {
            'model_type': 'Ensemble (Isolation Forest + Rule-Based + Statistical)',
            'n_estimators': 200,
            'contamination': self.contamination,
            'features_used': len(self.feature_columns),
            'feature_names': self.feature_columns,
            'training_stats': self.training_stats,
            'is_fitted': self.is_fitted
        }

    def save_model(self, path):
        """Save trained model to disk."""
        model_data = {
            'isolation_forest': self.isolation_forest,
            'scaler': self.scaler,
            'feature_columns': self.feature_columns,
            'training_stats': self.training_stats,
            'contamination': self.contamination,
            'is_fitted': self.is_fitted
        }
        with open(path, 'wb') as f:
            pickle.dump(model_data, f)
        print(f"Model saved to {path}")

    def load_model(self, path):
        """Load trained model from disk."""
        with open(path, 'rb') as f:
            model_data = pickle.load(f)
        self.isolation_forest = model_data['isolation_forest']
        self.scaler = model_data['scaler']
        self.feature_columns = model_data['feature_columns']
        self.training_stats = model_data['training_stats']
        self.contamination = model_data['contamination']
        self.is_fitted = model_data['is_fitted']
        print(f"Model loaded from {path}")
        return self


def train_and_evaluate(db_path):
    """Full training pipeline: load data, train, evaluate, save."""
    conn = sqlite3.connect(db_path)
    market_data = pd.read_sql("SELECT * FROM market_data", conn)
    conn.close()

    print(f"Loaded {len(market_data):,} market data records")
    print(f"Known anomalies: {(market_data['quality_flag'] != 'CLEAN').sum():,}")

    detector = MarketDataAnomalyDetector(contamination=0.05)
    detector.fit(market_data)

    results = detector.predict(market_data)

    print(f"\n--- Detection Results ---")
    print(f"Total records: {len(results):,}")
    print(f"ML anomalies: {results['ml_anomaly'].sum():,}")
    print(f"Rule-based flags: {(results['rule_flag_count'] > 0).sum():,}")
    print(f"Ensemble anomalies: {results['ensemble_anomaly'].sum():,}")
    print(f"\nSeverity Distribution:")
    print(results['severity'].value_counts().to_string())

    model_path = os.path.join(os.path.dirname(db_path), '..', 'models', 'anomaly_detector.pkl')
    detector.save_model(model_path)

    return detector, results


if __name__ == '__main__':
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'market_risk.db')
    train_and_evaluate(db_path)
