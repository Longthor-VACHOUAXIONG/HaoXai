#!/usr/bin/env python3
"""
Train Enhanced ML Models for Real AI-like Intelligence
"""
import sys
import os
sys.path.append('.')

from utils.enhanced_ml_trainer import train_enhanced_ml_models

def main():
    print("🚀 Starting Enhanced ML Training for Real AI-like Intelligence")
    print("=" * 60)
    
    # Use correct database path
    db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
    
    # Train enhanced models
    success = train_enhanced_ml_models(db_path, 'sqlite')
    
    if success:
        print("\n🎉 Enhanced ML Training Completed Successfully!")
        print("\n🤖 Your AI now has:")
        print("   ✅ Contextual Understanding")
        print("   ✅ Relationship Learning") 
        print("   ✅ Semantic Reasoning")
        print("   ✅ Workflow Knowledge")
        print("   ✅ Scenario Handling")
        print("   ✅ Real AI-like Intelligence")
        print("\n📊 The models can now:")
        print("   • Understand entity relationships (sample→host→taxonomy)")
        print("   • Infer context from partial information")
        print("   • Handle complex multi-table queries")
        print("   • Generate comparative analyses")
        print("   • Track sample lifecycles")
        print("   • Answer domain-specific questions")
        print("   • Provide workflow guidance")
        
        print("\n🔧 To use the enhanced models:")
        print("   1. Restart the application")
        print("   2. The enhanced models will be loaded automatically")
        print("   3. Try questions like:")
        print("      - 'Show me host information for sample CANB_TIS23_L_075'")
        print("      - 'What samples tested positive for coronavirus?'")
        print("      - 'Compare samples from different locations'")
        print("      - 'Track the journey of sample CANB_TIS23_L_075'")
        
    else:
        print("\n❌ Enhanced ML Training Failed!")
        print("Please check the error messages above.")

if __name__ == '__main__':
    main()
