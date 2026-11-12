      l_verify_id                    rsch.ext_checks.verify_id@rdb1%TYPE; -- id
      l_verify_num                   rsch.ext_checks.check_no@rdb1%TYPE; -- number
      l_verify_date                  rsch.ext_checks.verify_date@rdb1%TYPE; -- date
      l_item_id                     rsch.ext_items.item_id@rdb1%TYPE;
      l_item_num                    rsch.ext_items.item_num@rdb1%TYPE;
      l_item_date                   rsch.ext_items.item_date@rdb1%TYPE;
      l_item_kind                   rsch.ext_items.kind_code@rdb1%TYPE;

      l_pack_name                  rsch.ext_checks.batch_name@rdb1%TYPE;
      l_state                rsch.ext_checks.state_cd@rdb1%TYPE;

      --l_sys_flag                   rsch.ext_params.auto_flag@rdb1%TYPE;
      l_vendor_flag                 rsch.ext_vendors.auto_flag@rdb1%TYPE;

      l_post_date                     rsch.ext_items.post_date@rdb1%TYPE; -- gl date
