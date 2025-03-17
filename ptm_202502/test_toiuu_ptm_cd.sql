create table ptm_codinh_202502_cd as
        with goi_dadv as (select b.thuebao_id,b.goi_id, b.nhomtb_id, c.ten_goi, row_number() over (partition by thuebao_id order by  nhomtb_id desc)rnk
                                                  from css.v_bd_goi_dadv b , css.v_goi_dadv c
                                                  where b.goi_id=c.goi_id and b.trangthai=1 
                                                        and b.thang_bd >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) 		---thang n
											 and (c.nhomgoi_dadv_id<>2 or c.nhomgoi_dadv_id is null) 
					)
		, km_sd as (select a.thuebao_id, max(a.tyle_sd) tyle_sd 
								from css.v_khuyenmai_dbtb a, css.v_ct_khuyenmai b
								  where a.chitietkm_id = b.chitietkm_id and a.thang_bd >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		---thang n
												and a.hieuluc = 1 and ttdc_id = 0 and a.tyle_sd>0 and a.tyle_sd<>100
												and a.khuyenmai_id not in (1977, 2056, 2998, 2999) 
												and rownum = 1 --> đối với có đinh không cần table này
								  group by thuebao_id
							)
		, km_tb as (select a.thuebao_id, max(a.tyle_tb) tyle_tb 
						  from css.v_khuyenmai_dbtb a, css.v_ct_khuyenmai b
						  where a.chitietkm_id = b.chitietkm_id and a.thang_bd >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))   ---thang n
											and a.hieuluc=1 and ttdc_id = 0 and a.tyle_tb>0 and a.tyle_tb<>100 
											and a.khuyenmai_id not in (1977, 2056, 2998, 2999)
											and rownum = 1 --> đối với có đinh không cần table này
						 group by thuebao_id
					)
		, datcoc_goc as (select hd.hdtb_id, dc.thang_bd thang_bddc, dc.thang_kt thang_ktdc, dc.CUOC_DC DATCOC_CSD
                                                                    --      , case when nvl(ndc.tyle_vat_id, 1) in (3, 5) then kmb.DATCOC_CSD else round(kmb.DATCOC_CSD/1.1, 0) end DATCOC_CSD
                                                                                , dc.tien_td, kmb.huong_dc
            --                                                                , sum(cuoc_dc) datcoc_csd, sum(tien_td) tien_td 
                                                            from css.v_hd_thuebao hd, css.v_hdtb_datcoc dc, css.v_ct_khuyenmai kmb--, css_hcm.nhom_datcoc ndc
                                                            where hd.hdtb_id=dc.hdtb_id and hd.tthd_id = 6 and to_number(to_char(hd.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))	--thang n
                                                                            and dc.cuoc_dc>0 and (dc.nhom_datcoc_id not in (15,19, 20, 22, 24, 40) or dc.nhom_datcoc_id is null)
                                                                            and kmb.khuyenmai_id not in (1977, 2056, 2998, 2999) and kmb.chitietkm_id = dc.chitietkm_id-- and dc.nhom_datcoc_id =  ndc.nhom_datcoc_id (+)
            --                                                group by hd.hdtb_id, dc.thang_bd,dc.thang_kt
                                            union all
                                                    select a.hdtb_id, a.thang_bddc, a.thang_ktdc, a.datcoc_csd
                                                                    --      , case whennvl(ndc.tyle_vat_id, 1) in (3, 5) then kmb.DATCOC_CSD else round(kmb.DATCOC_CSD/1.1, 0) end DATCOC_CSD
                                                                    , a.tien_td, b.huong_dc
            --                                                    , sum(a.datcoc_csd) datcoc_csd,sum(a.tien_td) tien_td
                                                     from css.v_khuyenmai_dbtb a, css.v_ct_khuyenmai b, css.v_hd_thuebao c--, css_hcm.nhom_datcoc ndc
                                                     where c.hdtb_id = a.hdtb_id and a.chitietkm_id = b.chitietkm_id-- and dc.nhom_datcoc_id =  ndc.nhom_datcoc_id (+)
                                                                        and a.hieuluc = 1 and a.ttdc_id = 0
														  and a.datcoc_csd>0 and a.khuyenmai_id not in (1977, 2056, 2998, 2999) 
                                                                        and c.tthd_id = 6 and to_number(to_char(c.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))	--thang n
                                                                        and (b.nhom_datcoc_id not in (15,19,20,22,24, 40) or b.nhom_datcoc_id is null)                                             
                                                   --  group by a.hdtb_id, to_char(a.ngay_bddc, 'yyyymm'), to_char(a.ngay_ktdc, 'yyyymm') 
					)
        , datcoc as (select hdtb_id, min(thang_bddc) thang_bddc, min(thang_ktdc) thang_ktdc, min(huong_dc) huong_dc, sum(datcoc_csd) datcoc_csd, sum(tien_td) tien_td
                                from datcoc_goc
						  where rownum = 1 --> đối với có đinh không cần table này
                                group by hdtb_id
                                )
					--	  select * from datcoc_goc where hdtb_id = 24588507
		, pbh_nhan as	(select k.donvi_id, k.ten_dv, k.donvi_cha_id, h.ten_dv dv_cha 
									from admin.v_donvi k, admin.v_donvi h 
										where k.donvi_cha_id=h.donvi_id 
						) 
		, pbh_nhan_goc as (select khc.hdkh_id, khc.donvi_id, khc.nguoi_cn, k.donvi_cha_id, h.ten_dv dv_cha 
							from admin.v_donvi k, admin.v_donvi h, css.v_hd_khachhang khc 
							where k.donvi_id=khc.donvi_id and k.donvi_cha_id=h.donvi_id
							)
		, dvgt as (select thuebao_id, sum(cuoc_sd) cuoc_sd 
							from css.v_sudung_dvgt 
						    where dichvugt_id in (481,482,483,484,485)
						group by thuebao_id
						)
        , ungdung as (select a1.hdkh_id, a3.ghichu_tgdd, a3.ungdung_id
                                                    from css.v_hd_khachhang a1, css.v_hdkh_sub a3, css.ds_ungdung_online a4
                                                    where a1.hdkh_id = a3.hdkh_id and a3.ungdung_id = a4.ungdung_id
                                     )
        , v_db as (                            
                               select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
                                        , b.mucuoctb_id, cast(null as number) cuoc_tk, cast(null as number) cuoc_tc, cast(null as number) cuoc_tbi
								, cast(null as number) cuoc_ht, cast(null as number) cuoc_ip, cast(null as number) cuoc_nix, cast(null as number) cuoc_isp
								, a.chuquan_id
								, cast(null as number) cuoc_dt, cast(null as varchar(30)) ma_duan--, a.toanha_id, c.ma_duan
                             from css.v_db_thuebao b
									join css.v_db_cd a on a.thuebao_id = b.thuebao_id                                     
									join css.v_db_thanhtoan e on b.thanhtoan_id = e.thanhtoan_id
									join css.v_db_khachhang f on b.khachhang_id = f.khachhang_id
									--left join duan c on a.toanha_id = c.toanha_id
                             
                              union all
                             
                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
								, b.mucuoctb_id, null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp
								, a.chuquan_id
								, null cuoc_dt, null ma_duan
                             from css.v_db_gp a, css.v_db_thuebao b
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id
										and b.thanhtoan_id=e.thanhtoan_id --and a.chuquan_id in (145, 264, 266)
										
                              union all
                             
                             select b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
								, b.mucuoctb_id, null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp
								, a.chuquan_id
								, null cuoc_dt, null ma_duan
                             from css.v_db_thuebao b
									join css.v_db_ims a on a.thuebao_id = b.thuebao_id
									join css.v_db_thanhtoan e on b.thanhtoan_id = e.thanhtoan_id
									join css.v_db_khachhang f on b.khachhang_id = f.khachhang_id                                         

                             )

            , v_hdtb as (
                                    select a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb, cast(null as number) tocdo_id, cast(null as number) muccuoc_id
                                                    , cast(null as number) cuoc_tk, cast(null as number) cuoc_tc, cast(null as number) cuoc_tbi, cast(null as number) cuoc_ht
										  , cast(null as number) cuoc_ip, cast(null as number) cuoc_nix, cast(null as number) cuoc_isp, cast(null as number) cuoc_sd, cuoc_doitac
                                                    , cast(null as number) sl_mailing
                                                    , cast(null as number) phanloai_id, cast(null as number) cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den         
                                     from css.v_hdtb_cd a, css.v_hd_thuebao b
                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6 
                                     
                                    union all
                                    select a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb,null tocdo_id,null muccuoc_id
                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp, null cuoc_sd, null cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den
                                     from css.v_hdtb_gp a, css.v_hd_thuebao b
                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6                                      
                                  
                                      union all
                                    select  a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb,null tocdo_id,null muccuoc_id
                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp,null cuoc_sd, cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den
                                     from css.v_hdtb_ims a, css.v_hd_thuebao b
                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6 
                                     
                                     )
 
  select lhtb.loaihinh_tb dich_vu
                , a.ma_gd, a1.ma_gd ma_gd_gt
                , a.ma_kh, b.ma_tb, a.donvi_id tbh_nhan_id, pbh_nhan.ten_dv tobh_nhan
                , pbh_nhan.donvi_cha_id pbh_nhan_id
                , pbh_nhan.dv_cha phongbh_nhan
                , a.nhanvien_id, a.nguoi_cn nguoi_nhan, nvcapnhat.ten_nv ten_nguoi_nhan
                , pbh_nhan_goc.donvi_cha_id  pbh_nhan_goc_id
                , pbh_nhan_goc.dv_cha pbh_nhan_goc
                , pbh_nhan_goc.nguoi_cn nguoi_cn_goc 
                , kld.ten_kieuld
				
                , a.hdkh_id, a.hdkh_cha_id, b.hdtb_id, v_db.khachhang_id, v_db.thanhtoan_id, b.thuebao_id, b.thuebao_cha_id    
                , a.loaihd_id, a.kieuhd_id, a.kieutn_id, b.kieuld_id, b.dichvuvt_id, b.loaitb_id
                , (case when b.dichvuvt_id in (7, 8,9) then to_char(tocdo_kenh.tocdo)||tocdo_kenh.donvi else tocdo_adsl.ma_td end) ma_td
                , v_hdtb.tocdo_id, (case when a.loaihd_id=7 then v_db.mucuoctb_id
													when b.loaitb_id = 58 then v_db.mucuoctb_id			----update tu 13/12/2024 vi loc luon đôi tốc độ Fiber
														else b.mucuoctb_id end) mucuoctb_id                    
                , v_db.trangthaitb_id, v_db.doituong_id
				, dt.ten_dt ten_doituong 
                , b.doituong_ct_id--, dc.ap_id, dc.khu_id, dc.pho_id, dc.phuong_id, dc.quan_id, dc.sonha, dc.tinh_id 
                , b.ten_tb, b.diachi_ld, a.so_gt, v_db.mst, v_db.mst_tt, v_db.chuquan_id, regexp_replace (a.ma_duan, '\D', '') ma_duan_banhang
                , a.ngay_yc, b.ngay_ins as ngaycn_bbbg, b.ngay_kh, b.ngay_ht as ngay_bbbg, v_db.ngay_td, v_db.ngay_cat
                , (case when b.loaitb_id in (90,146) then 1 else v_hdtb.thoihan_id end) thoihan_id, v_hdtb.tg_thue_tu, v_hdtb.tg_thue_den
                , goi_dadv.goi_id goi_dadv_id, goi_dadv.ten_goi ten_goi_dadv, a.ctv_id, TRIM(UPPER(nvptm.ma_nv)) ma_tiepthi--, TRIM(UPPER(nvptm.ma_nv)) ma_tiepthi_new
                , nvptm.ten_nv ten_tiepthi, dm1.donvi_id to_tt_id, dm1.ten_dv to_tt, dm2.donvi_id donvi_tt_id, dm2.ten_dv donvi_tt, dm3.donvi_id donviql_tt_id, dm3.ten_dv donviql_tt 
                , a.nhanviengt_id, trim(upper(nvgt.ma_nv)) ma_nguoigt, nvgt.ten_nv nguoi_gt, dmgt1.ten_dv nhom_gt                            
               -- dat coc:
                , datcoc.thang_bddc, datcoc.thang_ktdc, datcoc.huong_dc sothang_dc
               -- , (case when datcoc.thang_bddc>0 and datcoc.thang_ktdc>0 then months_between(to_date(datcoc.thang_ktdc,'yyyymm'), to_date(datcoc.thang_bddc,'yyyymm'))+1 else null end) sothang_dc
                , (case when b.loaitb_id in (80,140) then datcoc_csd else round(datcoc.datcoc_csd/1.1,0) end) datcoc_csd
                , (case when b.loaitb_id in (80,140) then datcoc.tien_td else round(datcoc.tien_td/1.1,0) end) tien_td
--                , round(datcoc.datcoc_csd/1.1,0) datcoc_csd
--                , round(datcoc.datcoc_csd/1.1,0) tien_td
                , decode(b.loaitb_id,131, dvgt.cuoc_sd, v_db.cuoc_dt) cuoc_dt
                , f.tenmuc muccuoc, v_hdtb.cuoc_tn, v_hdtb.cuoc_doitac, v_hdtb.cuoc_sd, v_db.cuoc_tk, v_db.cuoc_tc, v_hdtb.cuoc_tbi
                , v_hdtb.cuoc_ht, v_hdtb.cuoc_ip cuoc_ip_mgwan, v_db.cuoc_nix, v_db.cuoc_isp, v_hdtb.phanloai_id
                , km_sd.tyle_sd, km_tb.tyle_tb, v_hdtb.sl_mailing, cast(null as number)muccuoc_tb, cast(null as number) tien_dvgt, cast(null as number) tien_tbi 
                , v_db.ma_duan, tocdo_adsl.soluong_ip, tocdo_adsl.sl_ip_mp
                , db_old.ngay_td ngay_td_kytruoc, trunc(b.ngay_ht)-trunc(db_old.ngay_td)+1 songay_tamngung
                , case when trunc(b.ngay_ht)-trunc(db_old.ngay_td) >=35 then 1 else 0 end duoctinh_ptm
--                , ungdung.ungdung_id, ungdung.ghichu_tgdd ghi_chu
                
   from  css.v_hd_khachhang a, css.v_hd_thuebao b, v_db, v_hdtb, tinhcuoc.v_dbtb db_old
                 , css.v_hd_khachhang a1
                 
                , css.muccuoc f             
                , css.loai_hd lhd
                , css.tocdo_adsl tocdo_adsl
                , css.tocdo_kenh tocdo_kenh
				, css.loaihinh_tb lhtb
				, css.kieu_ld kld
				, css.v_doituong dt
               
				, pbh_nhan, pbh_nhan_goc, dvgt
                , goi_dadv, km_sd, km_tb, datcoc--, ungdung
                
        --        , css.v_diachi_hdtb b1, css.v_diachi dc
               
                   , admin.v_nhanvien nvcapnhat 
                   , admin.v_nhanvien nvptm 
                   , admin.v_donvi dm1
                   , admin.v_donvi dm2
                   , admin.v_donvi dm3
                    
                 , admin.v_nhanvien nvgt
                 , admin.v_donvi dmgt1
                 , admin.v_donvi dmgt2      
                
         where a.hdkh_id = b.hdkh_id and b.thuebao_id = v_db.thuebao_id --and v_db.chuquan_id in (145, 264, 266)                 
			 and b.thuebao_id = db_old.thuebao_id (+)
			 and db_old.ky_cuoc (+) = 20250101 	---thang n-1
			 
				and b.dichvuvt_id in (1, 10,11)		---CD, GP
--				and b.dichvuvt_id in (4)		---BR
--				and b.dichvuvt_id in (7,8,9)	--TSL
--				and b.dichvuvt_id in (12, 13, 14, 15, 16, 26)	--CNTT
				
                and b.hdtb_id = v_hdtb.hdtb_id(+) and b.tthd_id in (6) and a.loaihd_id = lhd.loaihd_id
                and a.hdkh_cha_id = a1.hdkh_id (+)
                
                and v_hdtb.muccuoc_id = f.muccuoc_id(+) 
                and v_hdtb.tocdo_id=tocdo_adsl.tocdo_id(+) and v_hdtb.tocdo_id=tocdo_kenh.tocdo_id(+)
                
                and b.loaitb_id = lhtb.loaitb_id
				and b.kieuld_id = kld.kieuld_id
				and v_db.doituong_id = dt.doituong_id
                
                and a.donvi_id= pbh_nhan.donvi_id (+)
				and a.hdkh_cha_id = pbh_nhan_goc.hdkh_id (+)
				and b.thuebao_id = dvgt.thuebao_id (+)
                
                and b.thuebao_id= goi_dadv.thuebao_id(+) and goi_dadv.rnk(+) = 1
                and b.thuebao_id = km_sd.thuebao_id(+) 
                and b.thuebao_id = km_tb.thuebao_id(+)
                and b.hdtb_id = datcoc.hdtb_id(+)
           --     and a.hdkh_cha_id = ungdung.hdkh_id (+)
                
             --   and b.hdtb_id = b1.hdtb_id(+) --and b1.diachild_id = dc.diachi_id(+)
             
                and a.nhanvien_id = nvcapnhat.nhanvien_id(+) 
                and a.ctv_id = nvptm.nhanvien_id (+)
                and nvptm.donvi_id = dm1.donvi_id (+)
                and dm1.donvi_cha_id = dm2.donvi_id (+)
                and dm2.donvi_cha_id = dm3.donvi_id (+)                
                and a.nhanviengt_id = nvgt.nhanvien_id (+)
                and nvgt.donvi_id = dmgt1.donvi_id (+)
                and dmgt1.donvi_cha_id = dmgt2.donvi_id (+)
                
                and ( (a.loaihd_id=1 and b.kieuld_id not in (540,541,557,249,13130,13222,13224,71,280,550,551,13235, 14069) )  
                                      or (a.loaihd_id=6 and b.kieuld_id in (81,567,623,677,701,703,719,825,828,904,913,789,770,13258, 13244) and b.loaitb_id=77)  -- co dinh,vfone,bfone -> siptrunk
                                      or (a.loaihd_id=41 and b.kieuld_id not in (13179, 13261, 13286, 280, 13187, 14050) and b.loaitb_id in (140,80,116,117,55,122, 132, 154,153, 288, 40,2116 , 352, 373) )  
                                      or (a.loaihd_id=41 and b.kieuld_id in (13281,13189, 13236)  ) 
                                      or (b.kieuld_id=49 AND b.loaitb_id in (122, 175,2116,373,2116) )  -- Ban thiet bi/Ban goi dich vu
                                      or (a.loaihd_id=31 and b.kieuld_id=550 and b.loaitb_id=122 -- Phi duy tri
                                                and exists(select hdtb_id from css.v_hdtb_datcoc c where c.nhom_datcoc_id=12 and c.chitietkm_id=39325 and hdtb_id=b.hdtb_id))   
                                     or (b.kieuld_id in (96,13089) and db_old.trangthaitb_id=6)   -- tai lap     
                   -- or (a.loaihd_id=2 and b.kieuld_id=155 and b.loaitb_id in (58,61,171,271))   -- ccq                    
                    )        

			 and to_number(to_char(b.ngay_ht, 'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) 		--thang n
;