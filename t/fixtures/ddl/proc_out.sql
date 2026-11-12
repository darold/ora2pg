               rsch.rem_iface_pkg.get_item_info@rdb1 (
                  p_dept                   => ОТДЕЛ_ERP,
                  p_kind                => ГРУППА_ERP,
                  x_combo_id   => x_combo_id,
                  x_cat_id           => x_cat_id,
                  X_ERR_TEXT               => ERR_TEXT,
                  p_doc_type              => item_kind);
