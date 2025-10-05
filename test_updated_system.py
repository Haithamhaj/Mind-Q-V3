#!/usr/bin/env python3
"""
اختبار النظام بعد التحديث - البيانات المفقودة
"""

import sys
import os
from pathlib import Path

# إضافة مسار backend
backend_path = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_path))

try:
    import pandas as pd
    from app.services.phase0_quality_control import QualityControlService
    
    print("🧪 اختبار النظام المُحدث")
    print("="*50)
    
    # إنشاء بيانات اختبار مع مشاكل
    test_data = {
        'shipment_id': [f'SHP{i:03d}' for i in range(10)],
        'carrier': ['DHL', 'Aramex', 'SMSA'] * 3 + ['DHL'],
        'weight_kg': [15.2, None, 8.5, None, 12.1, None, 22.3, None, 5.8, None],  # 50% missing
        'status': ['Delivered'] * 8 + [None, None]  # 20% missing
    }
    
    df = pd.DataFrame(test_data)
    
    print(f"📊 البيانات: {len(df)} صف، {len(df.columns)} عمود")
    print("\n📈 البيانات المفقودة:")
    for col in df.columns:
        missing_pct = df[col].isnull().sum() / len(df)
        if missing_pct > 0:
            print(f"   {col}: {missing_pct:.1%}")
    
    # تشغيل Quality Control
    print("\n🔍 تشغيل Quality Control...")
    service = QualityControlService(df, key_columns=['shipment_id'])
    result = service.run()
    
    print(f"\n📊 النتيجة: {result.status}")
    print(f"⚠️  التحذيرات: {len(result.warnings)}")
    print(f"❌ الأخطاء: {len(result.errors)}")
    
    if result.warnings:
        print("\nالتحذيرات:")
        for warning in result.warnings:
            print(f"   ⚠️  {warning}")
    
    if result.errors:
        print("\nالأخطاء:")
        for error in result.errors:
            print(f"   ❌ {error}")
    
    # التحقق من النتيجة
    if result.status in ["WARN", "PASS"]:
        print("\n✅ النجاح: النظام لم يتوقف رغم وجود >20% بيانات مفقودة!")
        print("✅ سيتم معالجة البيانات المفقودة في Phase 5")
    else:
        print("\n❌ فشل: النظام ما زال يتوقف")
    
    print("\n" + "="*50)
    print("🎯 الاختبار مكتمل!")
    
except ImportError as e:
    print(f"❌ خطأ في الاستيراد: {e}")
    print("تأكد من وجود pandas والمكتبات المطلوبة")
except Exception as e:
    print(f"❌ خطأ غير متوقع: {e}")
    import traceback
    traceback.print_exc()







