create table ptm_codinh_202410 as
select * from ptm_codinh_202410_cntt
union all
select * from ptm_codinh_202410_tsl
union all
select * from ptm_codinh_202410_cd
union all
select * from ptm_codinh_202410_br
;
union all;
select * from ptm_codinh_202410;
create table ptm_codinh_202410_cntt as
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
												and (b.nhom_km in (1,12) or b.nhom_km is null) 
								  group by thuebao_id
							)
		, km_tb as (select a.thuebao_id, max(a.tyle_tb) tyle_tb 
						  from css.v_khuyenmai_dbtb a, css.v_ct_khuyenmai b
						  where a.chitietkm_id = b.chitietkm_id and a.thang_bd >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))   ---thang n
											and a.hieuluc=1 and ttdc_id = 0 and a.tyle_tb>0 and a.tyle_tb<>100 
											and a.khuyenmai_id not in (1977, 2056, 2998, 2999)
											and (b.nhom_km in (1,12) or b.nhom_km is null)
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
        , v_db as (select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
                                        , b.mucuoctb_id
								, a.chuquan_id                                       
                                        , cast(null as number) cuoc_dt
                                       , a.toanha_id, d.ma_duan
                             from css.v_db_adsl a, css.v_db_thuebao b
                                       , css.v_toanha c, css.v_duan d
                                       , css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id = b.thuebao_id and b.khachhang_id=f.khachhang_id and b.thanhtoan_id=e.thanhtoan_id
                                        and a.toanha_id=c.toanha_id(+)  and c.DUAN_ID=d.DUAN_ID(+) --and a.chuquan_id in (145, 264, 266)
                            
                             union all
                            
                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
                                        , b.mucuoctb_id
								, a.chuquan_id
								, null cuoc_dt, a.toanha_id, d.ma_duan
                             from css.v_db_cd a, css.v_db_thuebao b
                                       ,css.v_toanha c, css.v_duan d 
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id and b.thanhtoan_id=e.thanhtoan_id
                                        and a.toanha_id=c.toanha_id(+) and c.DUAN_ID=d.DUAN_ID(+) --and a.chuquan_id in (145, 264, 266)
                             
                              union all
                             
                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
								, b.mucuoctb_id
								, a.chuquan_id
								, null cuoc_dt, null toanha_id, null  ma_duan
                             from css.v_db_gp a, css.v_db_thuebao b
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id
										and b.thanhtoan_id=e.thanhtoan_id --and a.chuquan_id in (145, 264, 266)
                        
                               union all
                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
								, b.mucuoctb_id
								, a.chuquan_id
								, null cuoc_dt, a.toanha_id, d.ma_duan
                             from css.v_db_mgwan a, css.v_db_thuebao b
                                       ,css.v_toanha c, css.v_duan d 
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id and b.thanhtoan_id=e.thanhtoan_id
                                        and a.toanha_id=c.toanha_id(+) and c.DUAN_ID=d.DUAN_ID(+) --and a.chuquan_id in (145, 264, 266)
                             
                              union all
                             
                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
                                       , b.mucuoctb_id
							    , a.chuquan_id
							    , null cuoc_dt, a.toanha_id, d.ma_duan
                             from css.v_db_tsl a, css.v_db_thuebao b
                                       ,css.v_toanha c, css.v_duan d 
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id and b.thanhtoan_id=e.thanhtoan_id
                                        and a.toanha_id=c.toanha_id(+) and c.DUAN_ID=d.DUAN_ID(+) and a.daucuoi_id = 1 --and a.chuquan_id in (145, 264, 266)
                             
                              union all
                             
                             select b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
								, b.mucuoctb_id
								, a.chuquan_id
								, null cuoc_dt, a.toanha_id, d.ma_duan
                             from css.v_db_ims a, css.v_db_thuebao b
                                       ,css.v_toanha c, css.v_duan d 
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id and b.thanhtoan_id=e.thanhtoan_id                                        
                                        and a.toanha_id=c.toanha_id(+) and c.DUAN_ID=d.DUAN_ID(+) --and a.chuquan_id in (145, 264, 266)
                             
--                               union all
--                             
--                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id,b.ma_tb,b.dichvuvt_id, b.loaitb_id,b.ngay_sd, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id, 
--                                        f.so_gt, f.mst, e.mst mst_tt, f.loaikh_id, f.so_dt, 
--                                         null daucuoi_id,null tocdo_id,b.mucuoctb_id,null muccuoc_id,null ne_id,null madoicap,null matb_tn,null port_id,null vci_vpi_id,null ma_lt,
--                                         null ma_tb_sub,null cap_id,null vitri,null vitri_2,null ketcuoi_id,null doicap,null doicap_2,
--                                         null loaikenh_id,0 culy,null slid,null password,null seri_md, a.chuquan_id,null tramtb_id,null bras_id,null dslam_id,
--                                        goicuoc_id, null sl_cuocgoi, null tinhkhac,
--                                        null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_tkdt, null cuoc_tcdt, null cuoc_ip, null cuoc_nix, null cuoc_isp, null cuoc_sd, null cuoc_doitac,
--                                        null TOCDO_ISP,null  TOCDO_NIX,null  TOCDO_PIR_ID,null  cuoc_cir ,null cuoc_pir, null cuoc_bd,
--                                        null sl_mailing, null cuoc_dt, null ngay_duytri, null ngay_duytri_kt,
--                                        trangbi_id, null phanloai_id, null linhvuc_id, null cuoc_tn,null  LOAICAP_ID, null THONGTIN_TC, null sltv_htvc, null loainode_id,
--                                       null thoihan_id, b.tg_thue_tu, b.tg_thue_den, null toanha_id, null ma_duan
--                             from css.v_db_dd a, css.v_db_thuebao b
--                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
--                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id and b.thanhtoan_id=e.thanhtoan_id
                             
                             union all
                             
                             select  b.khachhang_id, b.thanhtoan_id, a.thuebao_id, b.ngay_td, b.ngay_cat, b.doituong_id, b.trangthaitb_id
                                        , f.mst, e.mst mst_tt
								, b.mucuoctb_id
								, a.chuquan_id
								, cuoc_dt, null toanha_id, null ma_duan
                             from css.v_db_cntt a, css.v_db_thuebao b
                                       ,css.v_db_thanhtoan e, css.v_db_khachhang f
                             where a.thuebao_id=b.thuebao_id and b.khachhang_id=f.khachhang_id
										and b.thanhtoan_id=e.thanhtoan_id --and a.chuquan_id in (145, 264, 266)
                             )

            , v_hdtb as (select a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb, a.tocdo_id, a.muccuoc_id
                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp, cuoc_sd, cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den                
                                     from css.v_hdtb_adsl a, css.v_hd_thuebao b
                                     where b.dichvuvt_id not in (7,8,9) and b.tthd_id = 6 and a.hdtb_id=b.hdtb_id 
                                     
                                    union all
                                    select a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb, null tocdo_id,null muccuoc_id
                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp, null cuoc_sd, cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den         
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
                                    select a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb,a.tocdo_id,null muccuoc_id 
                                                    , a.cuoc_tk, a.cuoc_tc, a.cuoc_tbi, a.cuoc_ht, a.cuoc_ip, a.cuoc_nix, a.cuoc_isp,null cuoc_sd, null cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den              
                                     from css.v_hdtb_mgwan a, css.v_hd_thuebao b
                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6 
                                     
                                      union all
                                    select a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb,a.tocdo_id,null muccuoc_id
                                                    , a.cuoc_tk, a.cuoc_tc, a.cuoc_tbi, a.cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp, null cuoc_sd, null cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den
                                     from css.v_hdtb_tsl a, css.v_hd_thuebao b
                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6 and daucuoi_id = 1
                                    
                                      union all
                                    select  a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb,null tocdo_id,null muccuoc_id
                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp,null cuoc_sd, cuoc_doitac
                                                    , null sl_mailing
                                                    , null phanloai_id, null cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den
                                     from css.v_hdtb_ims a, css.v_hd_thuebao b
                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6 
                                     
--                                    union all
--                                    select  a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
--                                                    , b.mucuoc_tb,null tocdo_id,null muccuoc_id
--                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp, null cuoc_sd, null cuoc_doitac
--                                                    , null sl_mailing
--                                                    , null phanloai_id, null cuoc_tn, null thoihan_id, b.tg_thue_tu, b.tg_thue_den
--                                     from css.v_hdtb_dd a, css.v_hd_thuebao b
--                                     where a.hdtb_id=b.hdtb_id and b.tthd_id = 6 
                                     
                                     union all
                                     select  a.hdtb_id, b.hdkh_id, b.hdtt_id, b.thuebao_id, b.ma_tb
                                                    , b.mucuoc_tb,a.tocdo_id,a.muccuoc_id
                                                    , null cuoc_tk, null cuoc_tc, null cuoc_tbi, null cuoc_ht, null cuoc_ip, null cuoc_nix, null cuoc_isp, cuoc_sd, cuoc_doitac
                                                    , sl_mailing
                                                    , phanloai_id, cuoc_tn, a.thoihan_id, b.tg_thue_tu, b.tg_thue_den
                                     from css.v_hdtb_cntt a, css.v_hd_thuebao b
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
                , v_hdtb.tocdo_id, (case when a.loaihd_id=7 then v_db.mucuoctb_id else b.mucuoctb_id end) mucuoctb_id                    
                , v_db.trangthaitb_id, v_db.doituong_id
				, dt.ten_dt ten_doituong 
                , b.doituong_ct_id, dc.ap_id, dc.khu_id, dc.pho_id, dc.phuong_id, dc.quan_id, dc.sonha, dc.tinh_id 
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
                , f.tenmuc muccuoc, v_hdtb.cuoc_tn, v_hdtb.cuoc_doitac, v_hdtb.cuoc_sd, v_hdtb.cuoc_tk, v_hdtb.cuoc_tc, v_hdtb.cuoc_tbi
                , v_hdtb.cuoc_ht, v_hdtb.cuoc_ip cuoc_ip_mgwan, v_hdtb.cuoc_nix, v_hdtb.cuoc_isp, v_hdtb.phanloai_id
                , km_sd.tyle_sd, km_tb.tyle_tb, v_hdtb.sl_mailing, cast(null as number)muccuoc_tb, cast(null as number) tien_dvgt, cast(null as number) tien_tbi 
                , v_db.ma_duan, tocdo_adsl.soluong_ip, tocdo_adsl.sl_ip_mp
                , db_old.ngay_td ngay_td_kytruoc, trunc(b.ngay_ht)-trunc(db_old.ngay_td)+1 songay_tamngung
                , case when trunc(b.ngay_ht)-trunc(db_old.ngay_td) >=35 then 1 else 0 end duoctinh_ptm
                , ungdung.ungdung_id, ungdung.ghichu_tgdd ghi_chu
                
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
                , goi_dadv, km_sd, km_tb, datcoc, ungdung
                
                , css.v_diachi_hdtb b1, css.v_diachi dc
               
                   , admin.v_nhanvien nvcapnhat 
                   , admin.v_nhanvien nvptm 
                   , admin.v_donvi dm1
                   , admin.v_donvi dm2
                   , admin.v_donvi dm3
                    
                 , admin.v_nhanvien nvgt
                 , admin.v_donvi dmgt1
                 , admin.v_donvi dmgt2      
                
         where a.hdkh_id = b.hdkh_id and b.thuebao_id = v_db.thuebao_id --and v_db.chuquan_id in (145, 264, 266) 
                and db_old.ky_cuoc (+) = 20240901 	---thang n-1
--				and b.dichvuvt_id in (1, 10,11)		---CD, GP
--				and b.dichvuvt_id in (4)		---BR
--				and b.dichvuvt_id in (7,8,9)	--TSL
				and b.dichvuvt_id in (12, 13, 14, 15, 16, 26)	--CNTT
				and b.thuebao_id = db_old.thuebao_id (+)
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
                and a.hdkh_cha_id = ungdung.hdkh_id (+)
                
                and b.hdtb_id = b1.hdtb_id(+) and b1.diachild_id = dc.diachi_id(+)
             
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
             
                          
             select * from ptm_codinh_202410
                where (ma_tb, ma_gd, hdtb_id) in 
                                                (select ma_tb, ma_gd, hdtb_id
                                                from ptm_codinh_202410 a 
                                                group by ma_tb, ma_gd, hdtb_id having count(*)>1 )
            ;
--Them cot------ tien_dnhm, tien_camket, tien_sodep:
                    ALTER TABLE ptm_codinh_202410 ADD (tien_dnhm NUMBER, tien_sodep NUMBER, tien_camket NUMBER)
				;
                    UPDATE ptm_codinh_202410 a 
                        SET tien_camket = (SELECT tien_ck FROM css.v_camket_hdtb WHERE hdtb_id=a.hdtb_id)
                        WHERE dichvuvt_id in (1,10,11)
                        ;
                    
                commit;
			 
                    MERGE INTO ptm_codinh_202410 a
                    USING (with dnhm as (SELECT dnhm1.khoanmuctt_id loai,dnhm1.hdtb_id, dnhm1.phieutt_id
                                                                          FROM css.v_ct_phieutt dnhm1, css.v_hd_thuebao hd
                                                                          WHERE hd.hdtb_id=dnhm1.hdtb_id AND dnhm1.khoanmuctt_id IN(1,2,13,17)
																and hd.tthd_id = 6			---update 1/6
                                                                                AND to_number(TO_CHAR(hd.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))			---thang n
                                                                       )
                                                    , thietbi as (SELECT thietbi1.khoanmuctt_id loai,thietbi1.hdtb_id, thietbi1.phieutt_id
                                                                                            FROM css.v_ct_phieutt thietbi1, css.v_hd_thuebao hd
                                                                                            WHERE hd.hdtb_id=thietbi1.hdtb_id AND thietbi1.khoanmuctt_id IN(5) 
																						and hd.tthd_id = 6			---update 1/6
                                                                                                    AND to_number(TO_CHAR(hd.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		--thang n
                                                                                           ) 
                                    SELECT hdtb_id, SUM(ct.tien+ct.tienkm) AS tien_dnhm
                                    FROM css.v_phieutt_hd hd1
                                                , (SELECT decode(ct.khoanmuctt_id,19,dnhm.loai,20, thietbi.loai,ct.khoanmuctt_id) loai
															, ct.khoanmuctt_id, ct.phieutt_id, ct.hdtb_id, thoaitra
															, decode(ct.khoanmuctt_id ,19,0,20,0,tien) tien
															, decode(ct.khoanmuctt_id,19,tien,20,tien,0) tienkm          
                                                    FROM css.v_ct_phieutt ct                     
                                                                LEFT JOIN
                                                                      dnhm ON ct.hdtb_id = dnhm.hdtb_id AND ct.phieutt_id = dnhm.phieutt_id AND ct.khoanmuctt_id = 19
                                                                LEFT JOIN
                                                                      thietbi ON ct.hdtb_id = thietbi.hdtb_id AND ct.phieutt_id = thietbi.phieutt_id AND ct.khoanmuctt_id = 20
                                                    WHERE EXISTS(SELECT 1 FROM css.v_hd_thuebao 
																		WHERE to_number(TO_CHAR(ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		--thang n
																						AND hdtb_id=ct.hdtb_id
																		)
                                                ) ct
                                        WHERE hd1.phieutt_id = ct.phieutt_id AND ct.loai IN (1,17) 
                                        GROUP BY hdtb_id) b
                    ON (a.hdtb_id = b.hdtb_id)
                    WHEN MATCHED THEN
                        UPDATE SET tien_dnhm = b.tien_dnhm
                    ;
                    commit;
                    
                    MERGE INTO ptm_codinh_202410 a
                                USING (SELECT hdtb_id, SUM(ct.tien+ct.tienkm) tien_sodep
                                                 FROM css.v_phieutt_hd hd1,       
                                                            (SELECT decode(ct.khoanmuctt_id,19,dnhm.loai,20,thietbi.loai,ct.khoanmuctt_id) loai 
																  , ct.khoanmuctt_id, ct.phieutt_id, ct.hdtb_id, thoaitra
																  , decode(ct.khoanmuctt_id ,19,0,20,0,tien) tien
																  , decode(ct.khoanmuctt_id,19,tien,20,tien,0) tienkm          
                                                            FROM css.v_ct_phieutt ct      
                                                                        LEFT JOIN (SELECT dnhm1.khoanmuctt_id loai,dnhm1.hdtb_id, dnhm1.phieutt_id
                                                                                              FROM css.v_ct_phieutt dnhm1
                                                                                              WHERE dnhm1.khoanmuctt_id IN(1,2,13,17)
                                                                                              ) dnhm ON ct.hdtb_id = dnhm.hdtb_id AND ct.phieutt_id = dnhm.phieutt_id AND ct.khoanmuctt_id = 19
                                                                        LEFT JOIN (SELECT thietbi1.khoanmuctt_id loai,thietbi1.hdtb_id, thietbi1.phieutt_id
                                                                                              FROM css.v_ct_phieutt thietbi1
                                                                                              WHERE thietbi1.khoanmuctt_id IN(5)
                                                                                              ) thietbi ON ct.hdtb_id = thietbi.hdtb_id AND ct.phieutt_id = thietbi.phieutt_id AND ct.khoanmuctt_id = 20
                                                              WHERE EXISTS(SELECT 1 FROM css.v_hd_thuebao 
																					WHERE to_number(TO_CHAR(ngay_ht, 'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		--thang n
																									AND hdtb_id=ct.hdtb_id
																			)
                                                            ) ct        
                                                WHERE hd1.phieutt_id = ct.phieutt_id AND ct.loai IN(23) 
                                                GROUP BY hdtb_id) b
                                ON (a.hdtb_id=b.hdtb_id)
                                WHEN MATCHED THEN
                                    UPDATE SET tien_sodep = b.tien_sodep
                                    ;
                                
                                
                                UPDATE ptm_codinh_202410 a
                                        SET tien_sodep = (SELECT cuoc_sd FROM css.v_hdtb_cntt where cuoc_sd>0 and hdtb_id = a.hdtb_id)
                                    -- SELECT ma_tb, tien_sodep FROM ttkd_bct.ptm_codinh_202410 a
                                    WHERE dichvuvt_id IN (13,14,15,16) AND EXISTS(SELECT 1 FROM css.v_hdtb_cntt where cuoc_sd>0 and hdtb_id=a.hdtb_id)
							 ;
                                
						  COMMIT;
            
			-- vi du: tsl db con 2 dong, 2 chu quan khac nhau, 2 goi tich hop co hieu luc cung ngay dk, ...
			select * from css_hcm.phieutt_hd hd where hdkh_id=21548061;

-- Mucuoc_tb:
        drop table temp_muccuoctb_thuebao purge;
	   ;
        create table temp_muccuoctb_thuebao as
                with km_dbtb as (select thuebao_id, tyle_tb, cuoc_tb, tien_tb , ngay_sd
												from css.v_khuyenmai_dbtb
                                                          where phanvung_id=28 and hieuluc=1 and ttdc_id = 0 and (tyle_tb>0 or cuoc_tb>0 or tien_tb>0) 
															and thang_bd >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) 	--thang n
                                                       ) 
					, hdtb as (select hdtb.thuebao_id, row_number() over (partition by hdtb.thuebao_id order by hdtb_id desc) rnk
									from css.v_hd_khachhang hdkh
													join css.v_hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id
									where hdkh.loaihd_id in (1,6,31,41,49) and hdtb.dichvuvt_id in (13,14,15,16) and hdtb.tthd_id = 6
													 and hdtb.loaitb_id = 39 --chi ap dung Key thuebao_id doi voi loaitb_id = 39
													and to_number(to_char(hdtb.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))  -- thang n
									)
                select db.thuebao_id, db.ma_tb, db.dichvuvt_id, db.loaitb_id, km_dbtb.tyle_tb, km_dbtb.cuoc_tb, km_dbtb.tien_tb, dbc.sl_mailing
							, (case when db.loaitb_id in (9,12,42,43,44,45,46,50,110,124,150,159,183)
							    then 
								   case when km_dbtb.tyle_tb>0 then round(mc.cuoc_tg * (100-nvl(km_dbtb.tyle_tb,0))/100, 0)  -- giam ty le, chua nhan so luong
										   when km_dbtb.cuoc_tb>0 then nvl(mc.cuoc_tg,0) - nvl(km_dbtb.cuoc_tb,0)  -- giam tien, da nhan so luong
										   when km_dbtb.tien_tb>0 then km_dbtb.tien_tb -- ap gia, chua nhan sl
										  else mc.cuoc_tg end
							    else case when km_dbtb.tyle_tb>0 then round(mc.cuoc_tg * (100-nvl(km_dbtb.tyle_tb,0))/100, 0) *  nvl(dbc.sl_mailing,1)  -- giam ty le, chua nhan so luong
										   when km_dbtb.cuoc_tb>0 then (nvl(mc.cuoc_tg,0) * nvl(dbc.sl_mailing,1) ) - nvl(km_dbtb.cuoc_tb,0)  -- giam tien, da nhan so luong
										   when km_dbtb.tien_tb>0 then km_dbtb.tien_tb * nvl(dbc.sl_mailing,1)   -- ap gia, chua nhan sl
										  else mc.cuoc_tg * nvl(dbc.sl_mailing,1) end
							    end) muccuoc_tb
        
            from hdtb, css.v_db_thuebao db, css.v_db_cntt dbc, css.v_muccuoc_tb mc
					, km_dbtb
            where dbc.phanvung_id=28 and mc.phanvung_id=28 
						 and hdtb.thuebao_id = db.thuebao_id and db.thuebao_id = dbc.thuebao_id and db.mucuoctb_id = mc.mucuoctb_id      
						 and dbc.thuebao_id = km_dbtb.thuebao_id(+) 
						and db.loaitb_id = 39 --chi ap dung Key thuebao_id doi voi loaitb_id = 39
						 and hdtb.rnk = 1
                ;
                
                drop table temp_muccuoctb_hdtb purge
			 ;
                create table temp_muccuoctb_hdtb as
                    with km_hdtb as (select hdtb1.hdtb_id, km_hdtb1.tyle_tb, km_hdtb1.cuoc_tb,  km_hdtb1.tien_tb
                                                            from css.v_hd_thuebao hdtb1, css.v_khuyenmai_hdtb km_hdtb1
                                                            where hdtb1.hdtb_id = km_hdtb1.hdtb_id
                                                                and hdtb1.tthd_id = 6 and to_number(to_char(hdtb1.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) 		--thang n
                                                                and (km_hdtb1.tyle_tb > 0 or km_hdtb1.cuoc_tb > 0 or km_hdtb1.tien_tb > 0)) 
                                
                select hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, km_hdtb.tyle_tb, mc.cuoc_tg, km_hdtb.cuoc_tb, km_hdtb.tien_tb, hdtbc.sl_mailing,
                            (case when hdtb.loaitb_id in (9,12,42,43,44,45,46,50,110,124,150,159,183)
                                        then 
                                            case when km_hdtb.tyle_tb>0 then (round(mc.cuoc_tg*(100-nvl(km_hdtb.tyle_tb,0))/100,0)) -- giam ty le
                                                     when km_hdtb.cuoc_tb>0 then nvl(mc.cuoc_tg,0) - nvl(km_hdtb.cuoc_tb,0)  -- giam tien
                                                     when km_hdtb.tien_tb>0 then km_hdtb.tien_tb  -- ap gia
                                                    else mc.cuoc_tg end
                                        else 
                                            case when km_hdtb.tyle_tb>0 then (round(mc.cuoc_tg*(100-nvl(km_hdtb.tyle_tb,0))/100,0)) * nvl(hdtbc.sl_mailing,1)  -- giam ty le
                                                     when km_hdtb.cuoc_tb>0 then (nvl(mc.cuoc_tg,0) * nvl(hdtbc.sl_mailing,1)) - nvl(km_hdtb.cuoc_tb,0)  -- giam tien
                                                     when km_hdtb.tien_tb>0 then km_hdtb.tien_tb * nvl(hdtbc.sl_mailing,1)   -- ap gia
                                                    else mc.cuoc_tg * nvl(hdtbc.sl_mailing,1) end
                                    end) muccuoc_tb
                from css.v_hd_khachhang hdkh, css.v_hd_thuebao hdtb, css.v_hdtb_cntt hdtbc, css.v_muccuoc_tb mc
                          , km_hdtb
                                
                    where hdkh.phanvung_id=28 and hdtb.phanvung_id=28 and hdtbc.phanvung_id=28 and mc.phanvung_id=28
                        and hdkh.hdkh_id=hdtb.hdkh_id and hdtb.hdtb_id=hdtbc.hdtb_id and hdtb.mucuoctb_id=mc.mucuoctb_id and hdtb.hdtb_id=km_hdtb.hdtb_id(+)     
                        and hdtb.tthd_id=6 and hdkh.loaihd_id in (1,6,31,41,49) 
				    and to_number(to_char(hdtb.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		--thang n
                        and hdtb.dichvuvt_id in (13,14,15,16)
					and loaitb_id not in (376) ---> dvu khong su dung muccuoc_tb
                ;
			  select * from temp_muccuoctb_thuebao;
			  select * from temp_muccuoctb_hdtb;
			  
			 select thuebao_id, loaitb_id,  count(*) from temp_muccuoctb_thuebao group by thuebao_id, loaitb_id having count(*)>1;
			 select hdtb_id,   count(*) from temp_muccuoctb_hdtb group by hdtb_id having count(*)>1;
			
                update ptm_codinh_202410 a 
                    set muccuoc_tb = (case when loaitb_id=39 then (select muccuoc_tb from temp_muccuoctb_thuebao where thuebao_id = a.thuebao_id ) 
																 else (select muccuoc_tb from temp_muccuoctb_hdtb where hdtb_id=a.hdtb_id ) 
												end) 
                    where dichvuvt_id in (13,14,15,16) 
                    ;
                    commit;

       
-- tien_dvgt theo hdtb_id lay thoi gian n-1
            drop table temp_tien_dvgt_theohdtb purge;
            create table temp_tien_dvgt_theohdtb as
                with b as (select hd.hdtb_id, km.chitietkm_id, km.cuoc_sd, km.tyle_sd, km.tien_sd, ctkm.dichvugt_id 
                                                from css.v_khuyenmai_dbtb km, css.v_ctkm_dvgt ctkm,  css.v_hd_thuebao hd
                                                where hd.hdtb_id=km.hdtb_id and km.chitietkm_id=ctkm.chitietkm_id and km.hieuluc=1 and ttdc_id = 0 
                                                    and nvl(km.tyle_tb,0)=0 and nvl(km.cuoc_tb,0)=0 and nvl(km.tien_tb,0)=0
                                                    and to_number(to_char(hd.ngay_ht,'yyyymm')) >= to_number(to_char(trunc(trunc(sysdate, 'month') - 1, 'month') -1, 'yyyymm')) 		--thang n-1
									)
						, c as (select a.hdtb_id, a.ma_tb, b.chitietkm_id, d.noidung soluong, d.cuoc_sd cuoc_sd_goc, b.cuoc_sd cuoc_giam_1donvi, b.tyle_sd, b.tien_sd                        
                                            , (case when b.tyle_sd>0 then round(d.cuoc_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1)  *(100-nvl(b.tyle_sd,0))/100,0)  -- giam tyle
                                                        when b.cuoc_sd>0 then  (d.cuoc_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1) ) - b.cuoc_sd    --  giam tien
                                                        when b.tien_sd>0 then b.tien_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1)  -- ap gia
													 else d.cuoc_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1) end
										) cuoc_sd                                   
									from css.v_hd_thuebao a, css.v_dangky_dvgt d, css.v_dichvu_gt e                                     
										    , b
						    where e.phanvung_id=28 and a.hdtb_id = d.hdtb_id and d.dichvugt_id = e.dichvugt_id                                              
								  and a.hdtb_id = b.hdtb_id(+) and d.dichvugt_id=b.dichvugt_id(+)
								  and a.loaitb_id = 39
								  and to_number(to_char(a.ngay_ht,'yyyymm')) >= to_number(to_char(trunc(trunc(sysdate, 'month') - 1, 'month') -1, 'yyyymm')) 		--thang n-1
						    )
                    select hdtb_id, sum(cuoc_sd) cuoc_sd
                        from  c 
				    group by hdtb_id
                ;     
--            create index temp_tien_dvgt_theohdtb_hdtb on temp_tien_dvgt_theohdtb (hdtb_id);
            
            drop table temp_tien_dvgt_theothuebao purge
		  ;
                    create table temp_tien_dvgt_theothuebao as
                            with b as (select hd.thuebao_id, km.chitietkm_id, km.cuoc_sd, km.tyle_sd, km.tien_sd, ctkm.dichvugt_id 
                                                        from css.v_khuyenmai_dbtb km, css.v_ctkm_dvgt ctkm,  css.v_hd_thuebao hd
                                                        where hd.hdtb_id=km.hdtb_id and km.chitietkm_id=ctkm.chitietkm_id and km.hieuluc=1 and km.ttdc_id = 0
                                                            and nvl(km.tyle_tb,0)=0 and nvl(km.cuoc_tb,0)=0 and nvl(km.tien_tb,0)=0
                                                            and to_number(to_char(hd.ngay_ht,'yyyymm')) >= to_number(to_char(trunc(trunc(sysdate, 'month') - 1, 'month') -1, 'yyyymm')) 	--thang n-1
										)
								, c as (select a.hdtb_id, a.ma_tb, b.chitietkm_id, d.noidung soluong, d.cuoc_sd cuoc_sd_goc, b.cuoc_sd cuoc_giam_1donvi, b.tyle_sd, b.tien_sd                        
												    , (case when b.tyle_sd>0 then round(d.cuoc_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1)  *(100-nvl(b.tyle_sd,0))/100,0)  -- giam tyle
															 when b.cuoc_sd>0 then  (d.cuoc_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1) ) - b.cuoc_sd    --  giam tien
															 when b.tien_sd>0 then b.tien_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1)  -- ap gia
															 else d.cuoc_sd * nvl(to_number(regexp_replace (d.noidung, '\D', '')),1) end
														) cuoc_sd
																	
												   from css.v_hd_thuebao a, css.v_dangky_dvgt d, css.v_dichvu_gt e                                     
														  ,  b
										  where e.phanvung_id = 28 and a.hdtb_id = d.hdtb_id and d.dichvugt_id = e.dichvugt_id                                              
												and a.thuebao_id = b.thuebao_id(+) and d.dichvugt_id=b.dichvugt_id(+)
												and a.loaitb_id = 39
												and to_number(to_char(a.ngay_ht,'yyyymm')) >= to_number(to_char(trunc(trunc(sysdate, 'month') - 1, 'month') -1, 'yyyymm')) 		--thang n-1
										  )
                            select hdtb_id, sum(cuoc_sd) cuoc_sd
						from  c
						group by hdtb_id
                ;
--                    create index temp_tien_dvgt_theothuebao_hdtb on temp_tien_dvgt_theothuebao (hdtb_id); 
                    
--                    update ptm_codinh_202410 set tien_dvgt='';
				
                    update ptm_codinh_202410 a 
                        set tien_dvgt = case when loaitb_id = 39 
																	then (select cuoc_sd from temp_tien_dvgt_theothuebao where hdtb_id=a.hdtb_id) 
                                                            else (select cuoc_sd from temp_tien_dvgt_theohdtb where hdtb_id=a.hdtb_id)  
											end 
                        -- select tien_dvgt from ptm_codinh_202410 a
                        where dichvuvt_id in (13,14,15,16) and loaitb_id=39
                        ;
                        commit;
                        
				   
-- tien_tbi: 
--                    UPDATE ptm_codinh_202410 SET tien_tbi=''
				;
                    MERGE INTO ptm_codinh_202410 a
                    USING (SELECT cttbi.hdtb_id, tbi_lhtb.loaitb_id, SUM(CASE WHEN tien_tratruoc>0 THEN cttbi.soluong*cttbi.tien_tratruoc ELSE cttbi.soluong*cttbi.tien END) tien_tbi
                                    FROM css.v_ct_mua_tbi cttbi, css.v_loai_tbi tbi, css.v_loai_tbi_lhtb tbi_lhtb , css.loaihinh_tb lhtb
                                    WHERE cttbi.loaitbi_id=tbi.loaitbi_id AND tbi.loaitbi_id=tbi_lhtb.loaitbi_id AND lhtb.loaitb_id=tbi_lhtb.loaitb_id
                                                        AND cttbi.loaitbi_id NOT IN (144,231,236,282,311,289,294,347,677,678,679)  --> loai tru cac loaitbi_id la thiet bi nhu Token, ..   
                                                        AND EXISTS(SELECT 1 FROM ptm_codinh_202410 
																			WHERE hdtb_id=cttbi.hdtb_id AND loaitb_id=tbi_lhtb.loaitb_id
																	)
                                    GROUP BY cttbi.hdtb_id, tbi_lhtb.loaitb_id
                                ) b ON (a.hdtb_id=b.hdtb_id AND a.loaitb_id=b.loaitb_id)
                    WHEN MATCHED THEN
                            UPDATE SET tien_tbi=b.tien_tbi
                    WHERE dichvuvt_id IN (13,14,15,16)
				;
                    
                    COMMIT;
                 
-- tien_dnhm, tien_camket, tien_sodep: da di chuyen thuc hien truc tiep tren DATAGUARD line 414
 -- Dat coc sau lap moi trong thang, cung ma_tiepthi voi hs lap moi:
                drop table temp_datcoc purge;
		
                create table temp_datcoc as
--              insert into temp_datcoc
		     with datcoc_goc as (select b.hdtb_id, a.thuebao_id , a.thang_bd thang_bddc, a.thang_kt thang_ktdc, a.cuoc_dc datcoc_csd, a.tien_td, ctkm.huong_dc
                                                                   from css.v_db_datcoc a, css.v_hdtb_datcoc b, css.v_ct_khuyenmai ctkm
                                                                    where a.thuebao_dc_id=b.thuebao_dc_id and a.chitietkm_id = ctkm.chitietkm_id and ctkm.khuyenmai_id not in (1977, 2056, 2998, 2999)
                                                                                and a.cuoc_dc > 0 and a.hieuluc = 1 and a.ttdc_id = 0 and b.thang_bd >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))			--thang n
                                                                                and (a.nhom_datcoc_id not in (15,19,20,22,24) or a.nhom_datcoc_id is null)
--                                                                    group by b.hdtb_id, a.thuebao_id, a.thang_bd, a.thang_kt
                                                                            
                                                                    union all                                  
                                                                    select a.hdtb_id, a.thuebao_id, a.thang_bddc, a.thang_ktdc, a.datcoc_csd, a.tien_td, b.huong_dc
                                                                    from css.v_khuyenmai_dbtb a, css.v_ct_khuyenmai b
                                                                    where a.chitietkm_id = b.chitietkm_id and a.datcoc_csd > 0 and  a.hieuluc = 1 and a.ttdc_id = 0
                                                                                and a.khuyenmai_id not in (1977, 2056, 2998, 2999) 
                                                                                and (b.nhom_datcoc_id not in (15,19,20,22,24) or b.nhom_datcoc_id is null)                                             
--                                                                    group by a.hdtb_id, a.thuebao_id, a.thang_bddc,a.thang_ktdc     
                                                )                           
                                , datcoc as (select max(hdtb_id) hdtb_id, thuebao_id, min(thang_bddc) thang_bddc, min(thang_ktdc) thang_ktdc
																, min(huong_dc) huong_dc, sum(datcoc_csd) datcoc_csd, sum(tien_td) tien_td
                                                        from datcoc_goc 
											 where datcoc_csd > 0 and thang_bddc >= to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) 			--thang n
                                                        group by  thuebao_id
                                                        )
--                                                        select * from datcoc where thuebao_id = 9607168
                select a.hdkh_id, b.hdtb_id, b.thuebao_id, a.ma_gd, b.ma_tb, b.loaitb_id, b.ngay_ht  
                                , datcoc.thang_bddc,datcoc.thang_ktdc
--                                , (case when datcoc.thang_bddc=0 or datcoc.thang_ktdc=0 then 0
--                                           when (datcoc.thang_bddc>0 and datcoc.thang_ktdc>0) then months_between(to_date(datcoc.thang_ktdc,'yyyymm'),to_date(datcoc.thang_bddc,'yyyymm'))+1
--                                 end) sothang_dc
                                ,(case when b.loaitb_id in (80,140) then datcoc_csd else round(datcoc.datcoc_csd/1.1,0) end) datcoc_csd
                                ,(case when b.loaitb_id in (80,140) then datcoc.tien_td else round(datcoc.tien_td/1.1,0) end) tien_td  
                                , datcoc.huong_dc sothang_dc
--                                , round(datcoc.datcoc_csd/1.1,0) datcoc_csd
--                                , round(datcoc.tien_td/1.1,0) tien_td
                                , a.ctv_id, c.ma_nv, c.ten_nv
                from css.v_hd_khachhang a, css.v_hd_thuebao b, admin.v_nhanvien c
                             , datcoc
                where a.hdkh_id=b.hdkh_id and a.ctv_id=c.nhanvien_id(+) and b.hdtb_id = datcoc.hdtb_id
                        and a.loaihd_id = 31 and b.kieuld_id = 550
                        and to_number(to_char(b.ngay_ht,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		---thang n
                        and exists(select 1 from ptm_codinh_202410 
													where datcoc_csd is null and thuebao_id=b.thuebao_id and ma_tiepthi=c.ma_nv 
										)
		;
            alter table ptm_codinh_202410 add hdtb_id_datcoc number
		  ;
		  
            update ptm_codinh_202410 a 
				set (hdtb_id_datcoc, datcoc_csd, tien_td, thang_bddc, thang_ktdc, sothang_dc)
							    = (select hdtb_id, datcoc_csd, tien_td, thang_bddc, thang_ktdc, sothang_dc
											from temp_datcoc 
											where datcoc_csd>0 and thuebao_id = a.thuebao_id and ma_nv = a.ma_tiepthi
								   )
                -- select thuebao_id, ma_tb, sothang_dc, datcoc_csd, tien_td from ptm_codinh_202410 a
                where datcoc_csd is null 
                    and exists(select 1 from temp_datcoc b
                                        where datcoc_csd>0 and thuebao_id=a.thuebao_id and ma_nv=a.ma_tiepthi)
                    and ngay_bbbg=(select max(ngay_bbbg) from ptm_codinh_202410 where thuebao_id=a.thuebao_id )
                    ;
                    
            ---recover datcoc_csd updated
--                update ptm_codinh_202410 set datcoc_csd = null 
----				select * from ttkd_bct.ptm_codinh_202410
--				where hdtb_id_datcoc is not null;
            commit;
            
--            select a.*, b.ma_tiepthi , b.DATCOC_CSD, b.SOTHANG_DC, b.hdtb_id_datcoc
--            from temp_datcoc a, ptm_codinh_202410 b
--            where a.thuebao_id=b.thuebao_id
            ;
            
------ thoihan_id
            update ptm_codinh_202410 set thoihan_id=1 
                where (thoihan_id<>1 or thoihan_id is null) and (tg_thue_tu is not null and tg_thue_den is not null )
                        and thoihan_id is null
            ;
		  


	-- Cuoc phat sinh:    ---chuyen len dataguard
				select * from tmp_ct_no;
				drop table tmp_ct_no purge
				;    
				create table tmp_ct_no as 
				select distinct a.* from bcss.v_ct_no a
				    where phanvung_id = 28 and ky_cuoc = 20241001 		--thang n
					   and khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) 
					   and exists(select 1 from ptm_codinh_202410 where thuebao_id=a.thuebao_id)
					   ;
				create index tmp_ct_no_tbid on tmp_ct_no (thuebao_id);
				
				alter table ptm_codinh_202410 add dthu_ps number(12)
--				update ptm_codinh_202410 a set dthu_ps=''
				;
				update ptm_codinh_202410 a 
				    set dthu_ps = (select sum(nogoc) from tmp_ct_no where thuebao_id=a.thuebao_id)
		;				
		commit;

	
			
				---select dthu_ps from ptm_codinh_202405 a
-- Dthu goi:
				alter table ptm_codinh_202410 
				    add (dthu_goi_goc number(12), dthu_goi number(12), dthu_goi_ngoaimang number(12))
				;
				update ptm_codinh_202410 bc set dthu_goi_goc='', dthu_goi='', dthu_goi_ngoaimang=''
				
				;
				--  select * from ttkd_bct.ptm_codinh_202410;
				
				update ptm_codinh_202410 bc set dthu_goi_goc=
				    (case
						-- 1800,1900: phi duy tri + phi tai nguyen + phi so dep
						when bc.loaitb_id in (38,127) and bc.thang_bddc is null 
								then (select nvl(cuoc_dt,0)+nvl(cuoc_tn,0)+nvl(cuoc_sd,0) from css.v_db_cntt where thuebao_id=bc.thuebao_id)
						when bc.loaitb_id in (38,127) and bc.thang_bddc is not null then nvl(bc.tien_td,0)+nvl(bc.cuoc_tn,0)  -- phi so dep thu 1 lan la tien tru dan
						
						-- CA  
						when bc.loaitb_id=116 and bc.goi_dadv_id in (15596,15599,15600,15601,15602,15603,15604,15605) then bc.datcoc_csd-250000
						when bc.loaitb_id=116 and (bc.goi_dadv_id is null or bc.goi_dadv_id not in (15596,15599,15600,15601,15602,15603,15604,15605)) then bc.datcoc_csd
						 
						
						-- CA-TAX,CA-IVAN,VNPT CA Sign Server,IVAN, VNPT SmartCA: tong tien hop dong tru tien token
						when bc.loaitb_id in (55,117,140,132,288,154) and bc.goi_dadv_id in (15596,15599,15600,15601,15602,15603,15604,15605) then bc.datcoc_csd-250000
						when bc.loaitb_id in (55,117,140,132,288,154) and (bc.goi_dadv_id not in (15596,15599,15600,15601,15602,15603,15604,15605) or bc.goi_dadv_id is null) then bc.datcoc_csd
						
				    
					    -- VNPT CA Plugin
					    when bc.loaitb_id in (80) and bc.thang_bddc is not null  then bc.datcoc_csd
					    when bc.loaitb_id in (80) and bc.thang_bddc is null then (select mc.cuoc_tg from css.v_muccuoc_tb mc where mc.mucuoctb_id = bc.mucuoctb_id) 
					    
					    
					    -- SmartCa PS
					    when bc.loaitb_id=318 then nvl(cuoc_dt,0)+nvl(tien_tbi,0)
				 
					    
					    -- VNPT Ky so
					    when bc.loaitb_id=181 then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
				    
					    
					   -- VNPT SSL: 78 (Chung thu so cong cong quoc te SSL)
						when bc.loaitb_id=78 and bc.thang_bddc is not null then bc.datcoc_csd
						
						-- POSIO: 294 (Dich vu quan ly ban hang): giong CA
					    when bc.loaitb_id in (294) then bc.datcoc_csd
					    
					    -- VNPT Smart IR: 324: giong CA
					    when bc.loaitb_id in (324) then bc.datcoc_csd
				  
					    
					  -- VNPT eKYC (He thong xac thuc va dinh danh dien tu) - theo thang va tra 1 lan thi heso_dv = 0.3
						when bc.loaitb_id=276 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=276 and thang_bddc is not null then bc.datcoc_csd  
				   
					   
					    -- HDDT, HDDT May tinh tien, VNPT Invoice - Inbot: gia tri hop dong
						when bc.loaitb_id in (122, 2116,373) and tocdo_id in (4784,5153) then nvl(bc.cuoc_dt,0)+nvl(tien_tbi,0) 
						when bc.loaitb_id in (122, 2116,373) and tocdo_id not in (4784,5153) then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
				
				
						-- VNPT BMIS: gia tri hop dong
						when bc.loaitb_id=286 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=286 and thang_bddc is not null then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)					   
				
						-- SMS Brandname: phi duy tri
						when bc.loaitb_id=131 then bc.dthu_ps
					   
						
						-- VNPT SmartCloud :  theo thang
						when bc.loaitb_id=153 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=153 and thang_bddc is not null then tien_td  --- luu y cac khoan ko co vat
						
						
						-- Mail Hosting:
						when bc.loaitb_id=159 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=159 and thang_bddc is not null then datcoc_csd  -- tien_td
					    
						
						-- My Cloud: theo thang 
						when bc.loaitb_id=152 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=152 and thang_bddc is not null then tien_td
						
						
						-- Premium Cloud: theo thang 
						when bc.loaitb_id in (40) and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id in (40) and thang_bddc is not null then tien_td
						
						
						-- VNPT Smart Ads: theo thang
						when bc.loaitb_id=56 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=56 and thang_bddc is not null then nvl(tien_td,0)+nvl(tien_dvgt,0)
						
						
						-- Web Hosting: 
						when bc.loaitb_id=12 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id=12 and thang_bddc is not null then datcoc_csd  -- la tong dat coc, co the 2 row dat coc
						
						
						-- Mail Server Ri�ng, Mail SMD, Mail VNN, Mail Offline, Mail Plus, Mail Secure, FMail
						when bc.loaitb_id in (44,9,50,124,42,43,45) and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)
						when bc.loaitb_id in (44,9,50,124,42,43,45) and thang_bddc is not null then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)
						
						
						-- VNPT Tracking:
						when bc.loaitb_id=144 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)
						when bc.loaitb_id=144 and thang_bddc is not null then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)
					   
					   
						-- VNPT Colocation:
						when bc.loaitb_id=39 and thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)
						when bc.loaitb_id=39 and thang_bddc is not null then nvl(tien_td,0)
					   
					   
						-- VNPT Check: 1 lan
						when bc.loaitb_id=52 then tien_tbi  -- tien_tbi la tong tien sau khi nhan voi so luong
						
						
						--  Ten mien VN, Ten mien quoc te:
						when bc.loaitb_id in (147,148) and bc.thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0) +nvl(tien_tbi,0)  ---update kyluong T3/24
						when bc.loaitb_id in (147,148) and bc.thang_bddc is not null then bc.datcoc_csd +nvl(tien_dvgt,0)			--update kyluong T3/24
						
						
						-- VPS: theo thang
						when bc.loaitb_id=111 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)        
				
				
						-- VNPT Edu: dich vu thu 1 lan = soluong * tientbi
						when bc.loaitb_id=118 then tien_tbi  -- tien_tbi la tong tien sau khi nhan voi so luong 
					  
					  
						-- VNPT Pos:
						when bc.loaitb_id=54 and bc.thang_bddc is not null then bc.datcoc_csd+nvl(bc.tien_dvgt,0)
						when bc.loaitb_id=54 and bc.thang_bddc is null then nvl(bc.tien_dvgt,0)+nvl(bc.tien_tbi,0)
				
				
						-- VNPT Pharmacy: tinh don gia 1 lan, cuocps tinh theo thang
						when bc.loaitb_id=161 and bc.thang_bddc is not null then bc.datcoc_csd+nvl(bc.tien_dvgt,0)
						
						
						-- Tong dai ao 3CX: theo thang,  VB 283/TTKD HCM-DH (goi 12,24,36... /thang)
							 -- Trong hop dong hop tac voi Sao Bac Dau. ve phi khoi tao: Sao Bac dau khong phan chi doanh thu cho TTKD nen khong the ghi nhan phi khoi tao
						when bc.loaitb_id=173 and bc.thang_bddc is not null then bc.tien_td+nvl(bc.tien_dvgt,0)
						when bc.loaitb_id=173 and bc.thang_bddc is null then nvl(bc.tien_dvgt,0)+nvl(bc.tien_tbi,0)
						
						
						-- Ecabinet: can xem lai khi co phat sinh
						when bc.loaitb_id=200 and bc.thang_bddc is not null then bc.datcoc_csd+nvl(bc.tien_dvgt,0)
						
						
						-- VNPT Meeting: 
						when bc.loaitb_id=89 and bc.thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)
						when bc.loaitb_id=89 and bc.thang_bddc is not null then datcoc_csd+nvl(tien_dvgt,0)
						
						
						-- iOffice:
						when bc.loaitb_id=35 and nvl(muccuoc_tb,0)=0 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)+nvl(tien_dnhm,0)  -- dthu tron hop dong + phi tich hop nhap o tien dnhm
						when bc.loaitb_id=35 and nvl(muccuoc_tb,0)>0 and datcoc_csd>0 then datcoc_csd+nvl(tien_dvgt,0)+nvl(tien_tbi,0)+nvl(tien_dnhm,0)                -- thue thang , nhung co dat coc , + phi tich hop nhap o tien dnhm => tinh nhu dthu tron hop dong         
						when bc.loaitb_id=35 and nvl(muccuoc_tb,0)>0 and datcoc_csd is null then muccuoc_tb+nvl(tien_dvgt,0)+nvl(tien_tbi,0)+nvl(tien_dnhm,0)      -- thue thang + phi tich hop nhap o tien dnhm         
						
						
						-- VNPT AI Camera:
						when bc.loaitb_id=285 and nvl(muccuoc_tb,0)=0 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)  -- dthu tron hop dong   
						when bc.loaitb_id=285 and nvl(muccuoc_tb,0)>0 then muccuoc_tb+nvl(tien_dvgt,0)+nvl(tien_tbi,0)      -- thue thang          
						
						
						-- Truyen dan t�n hieu truyen hinh:
						when bc.loaitb_id=146 and bc.thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)
						when bc.loaitb_id=146 and bc.thang_bddc is not null then datcoc_csd+nvl(tien_dvgt,0)
						
						
					   -- Hoi nghi truyen hinh NGN:
						when bc.loaitb_id=90 and phanloai_id=41 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)  -- theo phien
						when bc.loaitb_id=90 and phanloai_id=40 then datcoc_csd+nvl(tien_dvgt,0)   -- theo thang
				
				
					    -- Cac dv thuoc vnEdu:
					    when lower(dich_vu) like '%edu%' then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)
					    
					    
					    -- VNPT E-Learning: gia tri hop dong (dvu 1 lan)
					    when loaitb_id=208 and bc.thang_bddc is not null then nvl(datcoc_csd,0)+nvl(tien_tbi,0) 
					    when loaitb_id=208 and bc.thang_bddc is null then nvl(tien_dvgt,0)+nvl(tien_tbi,0) 
					    
					   ----VNPT iAlert: gia tri hop dong (dvu 1 lan) bsung Ky luong T4/24
					   when loaitb_id= 376 and bc.thang_bddc is not null then nvl(datcoc_csd,0)+nvl(tien_tbi,0) 
					    when loaitb_id=376 and bc.thang_bddc is null then nvl(tien_dvgt,0)+nvl(tien_tbi,0) 
					    
					    -- VNPT HIS, HMIS: theo thang	---> sua lai theo hop dong tra truoc, heso_dvu = 03. --Update T8/2024
					    when bc.loaitb_id=136 and bc.thang_bddc is null then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)
					    when bc.loaitb_id=136 and bc.thang_bddc is not null then  nvl(datcoc_csd,0) + nvl(tien_tbi,0)
				 
				 
					    -- VNPT eContract : gia tri hop dong giong HDDT
						when bc.loaitb_id=290 then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
						
						-- VNPT one Business: gia tri hop dong vb 46/TTr-DH 13/01/2022
						when bc.loaitb_id=292 then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
						
					   
						-- VNPT eReceipt: 175 (Bien Lai Dien Tu)
						when bc.loaitb_id=175 and tocdo_id in (13369) then nvl(bc.cuoc_dt,0)+nvl(tien_tbi,0) 
						when bc.loaitb_id=175 and tocdo_id not in (13369) then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
						
				 
						-- VNPT HKD: 302 (he thong ke toan/hddt ho kinh doanh ca the)
						when bc.loaitb_id=302 then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
						
						-- VNPT GoMeet: 322 (Phan mem hop truc tuyen)  bo sung ky luong T4/24
						when bc.loaitb_id=322 then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)  
						
				 
					    -- VNPT eZoZo: 280 (He thong quan ly nha hang) theo thang - 147/TTKD-HCM-?H 09/07/2021
					    when bc.loaitb_id=280 and nvl(muccuoc_tb,0)>0 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0) 
					    when bc.loaitb_id=280 and nvl(muccuoc_tb,0)=0 then nvl(tien_td,0)+nvl(tien_dvgt,0) 
				
					    
					    -- VNPT Home-Clinic: 296 (Phan mem Quan ly phong kham va bac sy gia dinh) theo thang : 1, theo g�i 6t,12t: 0.3 , VB 328/TTKD HCM-DH 31/12/2021
					    when bc.loaitb_id=296 and nvl(datcoc_csd,0)>0 and sothang_dc>=6 then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)     
					    when bc.loaitb_id=296 and (datcoc_csd is null or (nvl(datcoc_csd,0)>0 and sothang_dc<6)) then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0) 
					    
					    
					    -- vnFace: 284 (Phan mem cham cong, diem danh VNPT VnFace, la ung dung nhan dien khuon mat su dung cong nghe tri tue nhan tao (AI) ): theo thang
					    when bc.loaitb_id=284 and nvl(muccuoc_tb,0)>0 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0) 
					    when bc.loaitb_id=284 and nvl(muccuoc_tb,0)=0 then nvl(tien_td,0)+nvl(tien_dvgt,0) 					    
						 
					    -- VNPT AntiDDoS  : 
						  -- 847/TTr-DH - 13/08/2021, theo thang =1, theo thu� dich vu 72 gio = 0.3, eoffice 718660  
					    when bc.loaitb_id=279 and nvl(muccuoc_tb,0)=0 then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)  -- dthu tron hop dong, theo thue dich vu 72 gio
					    when bc.loaitb_id=279 and nvl(muccuoc_tb,0)>0 then muccuoc_tb+nvl(tien_dvgt,0)+nvl(tien_tbi,0)      -- thue thang                    
				
				
					    -- Bao mat KAS: 2113 (112/TTKD HCM-DH - thu nghiem)
					    when bc.loaitb_id=2113 and nvl(muccuoc_tb,0)=0 then null
					    when bc.loaitb_id=2113 and nvl(muccuoc_tb,0)>0 then null 
						
						
						-- VNPT AsMe: 277 (145/TTKD HCM-DH 07/07/2021) theo thang
						when bc.loaitb_id=277 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)        -- theo thang
						
						
						-- VNPT IQMS: 303 (269/TTKD HCM-DH 29/10/2021) theo thang
						when bc.loaitb_id=303 and nvl(muccuoc_tb,0)>0 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)    -- theo thang
						
				
						-- VNPT VXP: 235 (119 TTKD HCM-DH) thue phan mem theo thang hoac mua tron goi phan mem
						when bc.loaitb_id=235 and phanloai_id=95 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)          -- theo th�ng
						when bc.loaitb_id=235 and phanloai_id=96 then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)            -- Tron g�i
						  
				
					   -- VNPT eTicket: gia tri hop dong
						when bc.loaitb_id=319 and tocdo_id in (31895) then nvl(bc.cuoc_dt,0)+nvl(tien_tbi,0) 
						when bc.loaitb_id=319 and tocdo_id not in (31895) then nvl(bc.datcoc_csd,0)+nvl(bc.tien_tbi,0)
						
						
						-- VNPT eDIG: 
						when bc.loaitb_id=287 and phanloai_id=95 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)     --  theo th�ng
						when bc.loaitb_id=287 and phanloai_id=96 then nvl(datcoc_csd,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0)            --  Tron g�i
				   
				/*   
					    --Voice Brandname : Cuoc phat sinh binh quan trong thang n va thang n+1 (tinh doanh thu binh quan cua 2 thang n va n+1) (n la thang phat trien dich vu)     
							 --when bc.loaitb_id=358   ==> pbh gui file ngoai CT cho anh Nghia vao thang n+1
				
					   -- VNEdu eLession: chua ps, kiem tra lai khi phat sinh
							 -- 186/TTKD HCM-DH 16/08/2021 -  loai hop dong mua license phan mem
					    when bc.loaitb_id=227 and nvl(muccuoc_tb,0)>0 then nvl(muccuoc_tb,0)+nvl(tien_dvgt,0)+nvl(tien_tbi,0) 
					    when bc.loaitb_id=227 and nvl(muccuoc_tb,0)=0 then datcoc_csd+nvl(tien_dvgt,0) 
					   
					   -- VNPT AI Camera - phan mem he thong AI Camera gi�m s�t an ninh, giao th�ng
						  -- 881/TTr-DH 23/08/2021
							 Thu� phan mem theo th�ng => Doanh thu dich vu theo th�ng => he so dv = 1
							 Mua tron g�i phan mem => Gi� tri hop dong => hsdv = 0.3              
				
					    -- VNPT IOC: 317                   
					    -- VNPT CDN: 112
					    
					 */  
				 
						-- TSL:
						when bc.dichvuvt_id in (7,8,9) and tyle_sd is null then nvl(cuoc_tk,0)+nvl(cuoc_tc,0)+nvl(cuoc_nix,0)+nvl(cuoc_isp,0)  -- +cuoc_ip
						when bc.dichvuvt_id in (7,8,9) and tyle_sd is not null then round( (nvl(cuoc_tk,0)+nvl(cuoc_tc,0)+nvl(cuoc_nix,0)+nvl(cuoc_isp,0)) *(100- nvl(tyle_sd,0) )/100,0)  --+cuoc_tbi+cuoc_ht
				 
					    -- HTVC:
						when bc.dichvuvt_id=4 and bc.loaitb_id=18 
							 then (select cuoc_tb 
												from css.v_muccuoc_tb mc 
												where bc.dichvuvt_id=mc.dichvuvt_id and bc.tocdo_id=mc.tocdo_id
										)
				
					    -- Mega, Fiber tham gia goi tich hop:
						when bc.loaitb_id in (58,11) and bc.goi_dadv_id in (select goi_id 
																													from css.v_goi_dadv_lhtb b 
																													where phanvung_id=28 and b.goi_id=bc.goi_dadv_id and b.loaitb_id=bc.loaitb_id 
																																	and b.tocdo_id=bc.tocdo_id and (giamcuoc_tb=100 or giamcuoc_sd=100) 
																												) 
							 then
								    case when (select soluong_ip from css.tocdo_adsl where soluong_ip>0 and (sl_ip_mp is null or sl_ip_mp<soluong_ip) and tocdo_id=bc.tocdo_id)>=8                                                
												then (select tien_goi 
																	from css.v_goi_dadv_lhtb b 
																	where phanvung_id=28 and muccuoc_id = 1 and b.goi_id=bc.goi_dadv_id 
																	and b.loaitb_id=bc.loaitb_id and b.tocdo_id=bc.tocdo_id
														)+500000
										   when (select soluong_ip 
																from css.tocdo_adsl
																where soluong_ip>0 and (sl_ip_mp is null or sl_ip_mp<soluong_ip) and tocdo_id=bc.tocdo_id)=1
															then (select tien_goi 
																				from css.v_goi_dadv_lhtb b 
																				where phanvung_id=28 and muccuoc_id = 1 and b.goi_id=bc.goi_dadv_id 
																							and b.loaitb_id=bc.loaitb_id and b.tocdo_id=bc.tocdo_id 
																		)+200000 
										   else (select tien_goi 
															from css.v_goi_dadv_lhtb b 
															where phanvung_id=28 and muccuoc_id = 1 and b.goi_id=bc.goi_dadv_id 
																		and b.loaitb_id=bc.loaitb_id and b.tocdo_id=bc.tocdo_id
													)
								    end  
						    
						-- Mega, Fiber khong tham gia goi tich hop
						when bc.loaitb_id in (58,11) and (bc.goi_dadv_id is null 
																						or bc.goi_dadv_id not in (select goi_id 
																																	from css.v_goi_dadv_lhtb b 
																																	where phanvung_id=28 and b.goi_id=bc.goi_dadv_id and b.loaitb_id=bc.loaitb_id 
																																					and b.tocdo_id=bc.tocdo_id and (giamcuoc_tb=100 or giamcuoc_sd=100) 
																																) 
																				) then
							 case when tocdo_id in (44171, 44140) then 250000    -- Fiber VT c�ng �ch g�i FV60
									when (select soluong_ip 
													from css.tocdo_adsl
														where soluong_ip>0 and (sl_ip_mp is null or sl_ip_mp<soluong_ip) and tocdo_id=bc.tocdo_id)>=8   
										  then (select cuoc_tg 
															from css.v_muccuoc_tb mc 
															where mc.mucuoctb_id=bc.mucuoctb_id
													)+500000
									when (select soluong_ip 
														from css.tocdo_adsl
															where soluong_ip=1 and sl_ip_mp is null and tocdo_id=bc.tocdo_id)=1
										  then (select cuoc_tg 
														from css.v_muccuoc_tb mc 
														where mc.mucuoctb_id=bc.mucuoctb_id
													)+200000 
								    else (select cuoc_tg 
													from css.v_muccuoc_tb mc 
													where mc.dichvuvt_id=bc.dichvuvt_id and mc.mucuoctb_id=bc.mucuoctb_id
											)
							 end  
								    
						-- MyTV
						when bc.loaitb_id in (61,171) then
							 case 
								-- MyTV tham gia goi tich hop
								when bc.goi_dadv_id in (select goi_id 
																				from css.v_goi_dadv_lhtb b 
																				where phanvung_id=28 and b.goi_id=bc.goi_dadv_id 
																								and b.loaitb_id=bc.loaitb_id and b.tocdo_id=bc.tocdo_id and (giamcuoc_tb=100 or giamcuoc_sd=100) 
																		) 
									   then (select tien_goi 
															from css.v_goi_dadv_lhtb b 
															where phanvung_id=28 and b.goi_id=bc.goi_dadv_id 
																		and b.loaitb_id=bc.loaitb_id and b.tocdo_id=bc.tocdo_id
													)
								-- MyTV khong tham gia goi tich hop
								when (bc.goi_dadv_id is null 
												or bc.goi_dadv_id not in (select goi_id 
																								from css.v_goi_dadv_lhtb b 
																								where phanvung_id=28 and b.goi_id=bc.goi_dadv_id and b.loaitb_id=bc.loaitb_id 
																												and b.tocdo_id=bc.tocdo_id and (giamcuoc_tb=100 or giamcuoc_sd=100) ) 
																							)            
									   then (select cuoc_tb from css.v_muccuoc_tb mc where mc.mucuoctb_id=bc.mucuoctb_id)                     
							 end
								   
						-- Co dinh, Co dinh tinh khac, TKe thuong, TKe tuong tu, TK 2M, ISDN 2B+D, ISDN 30B+D, SipTrunking:
						when bc.dichvuvt_id in (1,10,11) then 
							 case 
								    when dichvuvt_id in (1,10,11) and loaitb_id<>77 and tien_camket>0 --and dthu_goi_goc < tien_camket
										  then tien_camket
										  
								    -- VFone 337/TTKD HCM-DH  13/08/2020: g�i B59 B99 B149 B249 B349 B449 B949
								    when loaitb_id=63 and bc.goi_dadv_id in (15648,15589,15590,15649,15591,15650,15592,15651,15593,15652,15594,15653,15595,15654)
											 then (select tien_goi 
															from css.v_goi_dadv_lhtb
																where phanvung_id=28 and goi_id=bc.goi_dadv_id and loaitb_id=bc.loaitb_id
														)
											 
								    -- Siptrunking:
								    when bc.loaitb_id=77 and goi_dadv_id = 15587 then 355000
									when bc.loaitb_id=77 then 250000 
								   
								    -- Trung ke thuong, trung ke tuong tu, ISDN 2B+D, ISDN 30B+D, trung ke 2M, tong dai:
								    when bc.loaitb_id in (3,4,5,6,14,15,16,17) then 119000
										  
								   -- codinh, codinh tinh khac, vfone, bfone,...
								   else 
										  case
												-- ca nhan co tham gia goi:
												when bc.doituong_id in (1,2,10,12,13,25,40,387) and bc.goi_dadv_id=3 then 40000   -- 20K
												when bc.doituong_id in (1,2,10,12,13,25,40,387) and bc.goi_dadv_id=4 then 80000  -- 66K
												when bc.doituong_id in (1,2,10,12,13,25,40,387) and bc.goi_dadv_id=5 then 110000  -- 99K
												when bc.doituong_id in (1,2,10,12,13,25,40,387) and bc.goi_dadv_id=63 then 65455  -- VNPOST
												when bc.doituong_id in (1,2,10,12,13,25,40,387) and (bc.goi_dadv_id is null or (bc.goi_dadv_id not in (3,4,5,63) )) then 20000
												when bc.doituong_id not in (1,2,10,12,13,25,40,387) then 119000
										  end
							 end  -- dichvuvt_id in (1,10,11)                    
						end)
				where loaitb_id not in (131,358)
			;
			commit;

-------- Tinh lai dthu_goi_goc cho thue bao vua lap moi co dinh vua chuyen sang Siptrunk trong thang:

			update ptm_codinh_202410 a 
					set (dthu_goi_goc, ghi_chu)=
									   (select case when goi_dadv_id = 15587 then 355000 - dthu_goi_goc
															else 250000 - dthu_goi_goc end, 'dthu goi=250000-dthu goi cua hs lap moi (lap moi, chuyen doi cung thang)' 
                                            from ptm_codinh_202410 b
										  where loaihd_id=1 and thuebao_id=a.thuebao_id                 
											  and exists (select 1 from ptm_codinh_202410 where thuebao_id=b.thuebao_id and loaihd_id=6)
											  )
		    where loaitb_id=77 and loaihd_id=6
					  and exists (select 1 from ptm_codinh_202410 where thuebao_id=a.thuebao_id and loaihd_id<>6)
								
				;
			
			update ptm_codinh_202410 set dthu_goi=dthu_goi_goc
			 
			;
            commit
			;
			update ptm_codinh_202410 a 
					    set dthu_goi_ngoaimang =
											 (select sum(nogoc) from bcss.v_tonghop
												where ky_cuoc = 20241001 --thang n
                                                        and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
							, dthu_goi = (select sum(nogoc) from bcss.v_tonghop
													   where ky_cuoc=20241001 --thang n
                                                       and khoanmuctc_id not in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb) 
					where loaitb_id = 131 
					
			;
					
					
					update ptm_codinh_202410 a 
					    set dthu_goi = dthu_ps
					    where thoihan_id = 1 and (dichvuvt_id in (1,10,11,7,8,9) or loaitb_id in (58,59,39) )
					    
			;
			commit;

-- ISDN 30B+D lon hon hoac bang 30 kenh/1 luong tu thang truoc => dthu goi =10K
			update ptm_codinh_202410 a 
						set dthu_goi_goc=10000, dthu_goi=10000
--			    select * from ptm_codinh_202410 a
                where loaitb_id in (15,17) 
				   and exists (select thuebao_cha_id, count(*) sl_socon 
                                    from tinhcuoc.v_dbtb 
                                     where ky_cuoc = 20240901 ---thang n-1
                                                    and thuebao_cha_id = a.thuebao_cha_id
                                     group by thuebao_cha_id having count(*)>=29     --tu 29 so con
								)
								
				;
			
			
			update ptm_codinh_202410 a 
				   set dthu_goi_goc = (select cuoc_tggoc + cuoc_ipgoc 
                                                from bcss.v_thftth 
                                                where phanvung_id=28 and ky_cuoc = 20241001 --thang n
                                                        and ma_tb = a.ma_tb)
					    , dthu_goi = (select cuoc_tggoc + cuoc_ipgoc 
                                            from bcss.v_thftth 
                                            where phanvung_id=28 and ky_cuoc = 20241001 --thang n
                                                    and ma_tb = a.ma_tb)
			    where loaitb_id in (58) and (thoihan_id = 2 or thoihan_id is null) and dthu_goi_goc is null
			    
			    
			 ;  
			   
			update ptm_codinh_202410
				   set dthu_goi_goc = muccuoc_tb
                        , dthu_goi = muccuoc_tb
			    where thoihan_id = 2 and chuquan_id in (145,266) and loaitb_id = 58 
					  and dthu_goi is null 
				;
			   
			update ptm_codinh_202410 
				   set dthu_goi = dthu_goi_goc
			    where loaitb_id = 277 and dthu_goi is null 
			    
			    ;                       
                        
			commit;


 ---move ve TTKDDB
		  create table ttkd_bct.ptm_codinh_202410 as select * from ttkd_hcm.ptm_codinh_202410@dataguard;
		  SELECT * FROM ttkd_bct.ptm_codinh_202410;

--- Rycle Bin DATAGUARD
		drop table ptm_codinh_202410_bs purge;
		drop table ptm_codinh_202410_cd purge;
		drop table ptm_codinh_202410_tsl purge;
		drop table ptm_codinh_202410_cntt purge;
		drop table ptm_codinh_202410_br purge;
		  
		  ----Update ma_nguoigt la daily
		  update ttkd_bct.ptm_codinh_202410 a
						set NHANVIENGT_ID= a.ctv_id
								, ma_nguoigt = upper(a.ma_tiepthi)
								, NGUOI_GT =  a.ten_tiepthi
								, NHOM_GT = a.donvi_tt
--					select * from ttkd_bct.ptm_codinh_202410 a
		  where upper(ma_tiepthi) like 'DL_%' or upper(ma_tiepthi)like'GTGT_%'  --and a.ma_tiepthi = 'GTGT00012'
		  ;
		  
		  DROP INDEX  ttkd_bct.idx_ptm_hdtbid;
		  create index ttkd_bct.idx_ptm_hdtbid on ttkd_bct.ptm_codinh_202410 (hdtb_id);
		  -- lydo_khongtinh_luong: Update ky luong T3/24
						alter table ttkd_bct.ptm_codinh_202410 add lydo_khongtinh_luong varchar2(200)
						;
						update ttkd_bct.ptm_codinh_202410 a
									set lydo_khongtinh_luong = (
														   with x as (
																			  select hdtb_id
																						   ,case when chuquan_id is null or chuquan_id not in (145,266,264) then ';kq1 Chu quan khong thuoc TTKD-HCM' end kq1
																						   ,case when doituong_id in (14,15,16,17,19,32,33,34,35,36,48,51,52,54,55,56,57,66) then ';kq2 Doi tuong nghiep vu' end kq2
																						   ,case when doituong_id in (47,49,50) and dichvuvt_id in (7,8,9) then ';kq3 TSL doi tuong VMS, STTTT, Bo CA' end kq3
																						   ,case when exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=b.loaitb_id) then ';kq4 Dich vu khong tinh luong' end kq4
																						   ,case when ( upper(ma_tiepthi)like'GTGT_%' 
																													or upper(ma_tiepthi)like'DAILY_%' or upper(ma_tiepthi)like'DL_%'
																													or upper(ma_nguoigt)like'GTGT_%' 
																													or upper(ma_nguoigt)like'DAILY_%' or upper(ma_nguoigt)like'DL_%')
																															then ';kq5 Phat trien qua Dai ly, chi tinh bsc cho Phong' 
																								end kq5
																						   ,case when thoihan_id=1 and ((loaitb_id in (58,59) and nvl(tien_dnhm,0)<2000000) or (dichvuvt_id in (7,8,9) and nvl(tien_dnhm,0)<3000000))
																												then ';kq6 TB ngan han nhung khong thu tien DNHM dung qui dinh' 
																								 end kq6
																						  ,case when loaihd_id=41 and loaitb_id=153 and nvl(sothang_dc,0)<6 
																											then ';kq7 Ghi nhan dthu khi gia han goi>=6 thang' 
																									end kq7       
																			  from ttkd_bct.ptm_codinh_202410 b
																			)                                        
													  select nvl(kq1,'') || nvl(kq2,'') || nvl(kq3,'') || nvl(kq4,'') || nvl(kq5,'') || nvl(kq6,'') || nvl(kq7,'') 
													  from x
													  where x.hdtb_id = a.hdtb_id
									) 
						;
		 
                    
                    commit;
                 
                    -- nv ptm:   
                    alter table ttkd_bct.ptm_codinh_202410
                        add (manv_ptm varchar2(20), tennv_ptm varchar2(100)
							, ma_pb varchar2(20), ten_pb varchar2(100), ma_to varchar2(20), ten_to varchar2(100)
							, ten_vtcv varchar2(100), ma_vtcv varchar2(20), loainv_id number, ten_loainv varchar2(100), loai_ld varchar2(100)
							, manv_hotro varchar2(20), tyle_hotro number(5,2), tyle_am number(5,2),nhom_tiepthi number )
                    ;
				-- NV TTKD:
				-- Nhom TL da xin y kien GD 22/04/2024 tinh 50% cho Huong BHOL cong doan hoan tat thu tuc dich vu SmartCA ko co ma tiep thi (KH vang lai) va co dthu :
				insert into ttkd_bsc.ptm_xuly_50_BHOL 
						 select 202410 thang, hdtb_id, thuebao_id, loaitb_id, ma_tiepthi, pbh_nhan_id
									, nhanvien_id, datcoc_csd, 'Tinh 50% MANV VNP017772 neu tbao hok co ma_tiepthi va BHOL xu ly don hang only loaitb_id_288' ghi_chu
						 from ttkd_bct.ptm_codinh_202410 a
						    where loaitb_id=288 and ma_tiepthi is null 
							   and pbh_nhan_id=2941 and nhanvien_id=1077 and datcoc_csd>0
				 ;
				 select * from ttkd_bsc.ptm_xuly_50_BHOL 
				 ;
				update ttkd_bct.ptm_codinh_202410 a 
				    set manv_ptm =(select ma_nv from admin_hcm.nhanvien_onebss where nhanvien_id=a.nhanvien_id)  --manv_ptm='VNP017772'
--				     select ma_tiepthi, manv_ptm from ttkd_bct.ptm_codinh_202410 a
				    where loaitb_id = 288 and ma_tiepthi is null 
					   and pbh_nhan_id=2941 and nhanvien_id=1077 and datcoc_csd>0
					   ; 
				
				update ttkd_bct.ptm_codinh_202410 a 
				    set manv_ptm = ma_tiepthi
				    where chuquan_id in (145,266,264) and manv_ptm is null
				;
				update ttkd_bct.ptm_codinh_202410 a 
				    set (tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ten_vtcv, ma_vtcv, loai_ld, nhom_tiepthi)
						= (select b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb, b.ten_vtcv, b.ma_vtcv, b.loai_ld, b.nhomld_id
							  from ttkd_bsc.nhanvien b
							  where b.thang = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) --thang n
											--and b.ma_pb=pb.ma_pb 
											and b.ma_nv = a.manv_ptm
							  ) 
				    where chuquan_id in (145,266,264) and tennv_ptm is null

;
			
				----END
					   ;
				
					-- select * from ttkd_bsc.nhanvien;
				commit;
-- Tao ds dai ly thang va ins dai ly moi:
			--****-- Dai ly hien huu:
--			delete from dm_daily_khdn where thang='202410' ;
			select * from  ttkd_bsc.dm_daily_khdn where thang = 202410;
			
			insert into ttkd_bsc.dm_daily_khdn --(ma_daily,ten_daily,manv_qldaily,ma_pb,thang,thang_kyhd,ma_to)
																(thang, ma_daily, ten_daily, manv_qldaily, ma_to, ma_pb, ma_vtcv, ten_vtcv, thang_kyhd, ghichu)
			    select 202410, ma_daily, ten_daily, manv_qldaily, ma_to, ma_pb, ma_vtcv, ten_vtcv, thang_kyhd, ghichu
								
			    from ttkd_bsc.dm_daily_khdn a
			    where a.thang = 202409
						 and not exists (select 1 from ttkd_bsc.dm_daily_khdn where thang > a.thang and ma_daily=a.ma_daily)
			 ;
			  --****-- Dai ly moi:
					insert into ttkd_bsc.dm_daily_khdn (ma_daily,ten_daily,ma_pb, ma_to, manv_qldaily, thang, thang_kyhd, loai_hopdong )
					    select distinct ma_tiepthi ma_daily, (select upper(ten_nv) from admin_hcm.nhanvien_onebss where ma_nv = a.ma_tiepthi) ten_daily,
								 ma_pb, ma_to, ma_nv, 202410, 202410, 'DAI LY MOI' loai_hopdong
					    from 
						   (
						   ---khong bao gio co theo doi T4,T5,T6
						   select distinct ma_tiepthi, (select ma_dv from admin_hcm.donvi where donvi_id = a.pbh_nhan_id) ma_pb,  ma_to, phongbh_nhan
										   , case when substr(ma_nguoigt,1,3) in ('VNP','CTV') then ma_tiepthi else null end ma_nv                             
							  from ttkd_bct.ptm_codinh_202410 a
							  where ma_tiepthi like 'GT%' and ma_tiepthi not in (select ma_daily from ttkd_bsc.dm_daily_khdn)
							  
						   ---khong bao gio co theo doi T4,T5,T6
						   union all
						   select distinct ma_tiepthi, (select ma_dv from admin_hcm.donvi where donvi_id=a.pbh_nhan_id) ma_pb,  ma_to, phongbh_nhan, 
											  case when substr(ma_nguoigt,1,3) in ('VNP','CTV') then ma_tiepthi else null end 
							  from ttkd_bct.ptm_codinh_202410 a
							  where ma_tiepthi like 'DL%' and ma_tiepthi not in (select ma_daily from ttkd_bsc.dm_daily_khdn)
						   
						   union all
						   select distinct ma_nguoigt, (select ma_dv from admin_hcm.donvi where donvi_id=a.pbh_nhan_id) ma_pb,  ma_to, phongbh_nhan , 
											   case when substr(ma_tiepthi,1,3) in ('VNP','CTV') then ma_tiepthi else null end
							  from ttkd_bct.ptm_codinh_202410 a
							  where ma_nguoigt like 'DL%' and ma_nguoigt not in (select ma_daily from ttkd_bsc.dm_daily_khdn)
							  
						   union all
						   select distinct ma_nguoigt, (select ma_dv from admin_hcm.donvi where donvi_id=a.pbh_nhan_id) ma_pb,  ma_to, phongbh_nhan,
											  case when substr(ma_tiepthi,1,3) in ('VNP','CTV') then ma_tiepthi else null end
							  from ttkd_bct.ptm_codinh_202410 a
							  where ma_nguoigt like 'GT%' and ma_nguoigt not in (select ma_daily from ttkd_bsc.dm_daily_khdn)          
					    ) a
					    ;
					    commit;
			
					---nho check before update
					update ttkd_bct.ptm_codinh_202410 a
					    set
						    ma_pb=(select ma_pb from ttkd_bsc.dm_phongban where active=1 and ma_pb_pttb=a.pbh_nhan_goc_id)
						   , ten_pb=(select ten_pb from ttkd_bsc.dm_phongban where active=1 and ma_pb_pttb=a.pbh_nhan_goc_id)
--					     select MA_GD, pbh_nhan_id, pbh_nhan_goc_id, donvi_tt_id, ma_pb, ten_pb from ttkd_bct.ptm_codinh_202410 a
					    where chuquan_id in (145,266)
									and donvi_tt_id=283427 and (pbh_nhan_goc_id in (284316,2943,11352,2942,2947,2944,2945,284317,2946)
																						or pbh_nhan_id in (284316,2943,11352,2942,2947,2944,2945,284317,2946)
																						)
					;
						   
						   select donvi_id, ten_dv from admin_Hcm.donvi where donvi_id in (2948, 284316,2943,11352,2942,2947,2944,2945,284317,2946, 2948,11563,11564, 283427, 11298);
										   
					
					update ttkd_bct.ptm_codinh_202410 a
					    set  --pbh_ptm_id=(select pbh_id from ttkd_bsc.dm_phongban where active=1 and ma_pb_pttb = a.pbh_nhan_id)
							   ma_pb = (select ma_dv from admin_Hcm.donvi where donvi_id = a.pbh_nhan_id)
							  , ten_pb = (select ten_dv from admin_Hcm.donvi where donvi_id = a.pbh_nhan_id)
--					     select ma_tiepthi, ma_gd, ma_nguoigt, (select ma_dv from admin_Hcm.donvi where donvi_id = a.pbh_nhan_id)ma_dv, ma_pb, ten_pb, pbh_nhan_id from ttkd_bct.ptm_codinh_202410 a
					    where chuquan_id in (145,266,264) and ma_pb is null --and pbh_nhan_id in (2948,11563,11564)
						    and (ma_tiepthi is null or ma_nguoigt like 'GTGT%' or ma_nguoigt like '%DL_%')
					;
					commit;
			-- PTTT:
				update ttkd_bct.ptm_codinh_202410 a 
				    set  ma_pb = (select ma_dv from admin_Hcm.donvi where donvi_id = 11298)
						  , ten_pb = (select ten_dv from admin_Hcm.donvi where donvi_id = 11298)
--				    select ma_gd, (select ten_dv from admin_Hcm.donvi where donvi_id = 11298) ten_dv, pbh_nhan_id, pbh_nhan_goc_id from ttkd_bct.ptm_codinh_202410 a
				    where chuquan_id in (145,266) and ma_pb is null 
									  and ((pbh_nhan_id=11298 and pbh_nhan_goc_id is null)
											 or (pbh_nhan_id=11298 and pbh_nhan_goc_id not in (11352,11563,11564,11578,284316,284317,2941,2942,2943,2944,2945,2946,2947,2948)))
					;

			commit;
				-- KTNV: test khong tinh
				update ttkd_bct.ptm_codinh_202410 a 
				    set ma_pb=null,ten_pb=null,ma_to=null, ten_to=null, manv_ptm=null, ma_vtcv=null,loainv_id=null,ten_loainv=null,loai_ld=null
--				      select ma_gd, (select ten_dv from admin_Hcm.donvi where donvi_id = 11298) ten_dv, pbh_nhan_id, pbh_nhan_goc_id, ten_tb, ma_pb, manv_ptm from ttkd_bct.ptm_codinh_202410 a
				    where chuquan_id in (145,266) and ( (ma_pb='VNP0700700' and pbh_nhan_id<>283530) or upper(ten_tb) like '%ERP TEST%')
			;
				-- TTVT ptm nhung ko co ma_tiepthi hoac ma_tiepthi sai/nghi viec: 
				update ttkd_bct.ptm_codinh_202410 a
				    set ma_pb = (select ma_dv from admin_hcm.donvi where donvi_id = a.donvi_tt_id)
--				     select ma_tiepthi, ma_pb, pbh_nhan_id, donvi_tt_id, donviql_tt_id, (select ma_dv from admin_hcm.donvi where donvi_id = a.donvi_tt_id) ma_pb_new from ttkd_bct.ptm_codinh_202410 a
				    where ma_pb is null and chuquan_id in (145,266)
					   and (pbh_nhan_id in (283451,283452,283453,283454,283455,283466,283467,283468,283469)
							 or donvi_tt_id in (283451,283452,283453,283454,283455,283466,283467,283468,283469)
							 or donviql_tt_id in (283451,283452,283453,283454,283455,283466,283467,283468,283469) )
							 ;

					-- Don vi phat trien la cac PBH nhung ko co ma_tiepthi hoac ma_tiepthi sai/nghi viec: 
					update ttkd_bct.ptm_codinh_202410 a
					    set  
							  ma_pb = (select ma_dv from admin_Hcm.donvi where donvi_id = a.DONVI_TT_ID)
							  , ten_pb = (select ten_dv from admin_Hcm.donvi where donvi_id = a.DONVI_TT_ID)
--					      select ma_gd, ma_tiepthi, ma_pb, pbh_nhan_id, donvi_tt_id, donviql_tt_id, (select ma_dv ||ten_dv from admin_Hcm.donvi where donvi_id = a.DONVI_TT_ID) from ttkd_bct.ptm_codinh_202410 a
					    where ma_pb is null and chuquan_id in (145,266,264)
							  and DONVI_TT_ID is not null
						;
					update ttkd_bct.ptm_codinh_202410 a
					    set  
							  ma_pb = (select ma_dv from admin_Hcm.donvi where donvi_id = a.pbh_nhan_id)
							  , ten_pb = (select ten_dv from admin_Hcm.donvi where donvi_id = a.pbh_nhan_id)
--					      select ma_gd, ma_tiepthi, manv_ptm, ma_pb, pbh_nhan_id, donvi_tt_id, donviql_tt_id, (select ma_dv ||ten_dv from admin_Hcm.donvi where donvi_id = a.pbh_nhan_id) from ttkd_bct.ptm_codinh_202410 a
					    where ma_pb is null and chuquan_id in (145,266,264)
							  and pbh_nhan_id is not null
						;
						commit;
						rollback;


				update ttkd_bct.ptm_codinh_202410 a set manv_hotro='', tyle_hotro='', tyle_am='' where ma_duan_banhang is not null
				;	

				--select * from ttkdhcm_ktnv.amas_booking_presale where ma_yeucau = 228705;
					MERGE into ttkd_bct.ptm_codinh_202410 a
							using (
												with 
													yc_dv as (select ma_yeucau, id_ycdv, ma_dichvu, row_number() over(partition by MA_YEUCAU, MA_DICHVU order by NGAYCAPNHAT desc) rnk
																	from ttkdhcm_ktnv.amas_yeucau_dichvu
																	where MA_HIENTRANG <> 14
																	)
													, t as	 (select c.manv_presale_hrm, c.tyle/100 tyle_hotro, decode(c.tyle_am,0,1,c.tyle_am/100) tyle_am, d.loaitb_id_obss, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom, b.id_ycdv
																		, c.NGAYHEN, c.NGAYCAPNHAT, c.NGAYNHANTIN_PS, c.NGAYXACNHAN, c.ps_truong
																 from yc_dv b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
																					where b.ma_yeucau=c.ma_yeucau and b.id_ycdv=c.id_ycdv and b.ma_dichvu = d.loaitb_id
																								and c.tyle>0 and c.ps_truong=1 and c.xacnhan=1  
																)
--																select * from t
																
												select MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, TYLE_AM, MA_DICHVU--, sum(tyle_hotro) tyle_hotro, sum(TYLE_NHOM) TYLE_NHOM --
																, decode(sum(ps_truong), 1, sum(tyle_hotro), max(tyle_hotro)) tyle_hotro
																, decode(sum(ps_truong), 1, sum(TYLE_NHOM), max(TYLE_NHOM)) TYLE_NHOM
															from t
															-- where ma_yeucau  in (213513)
															group by MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, TYLE_AM, MA_DICHVU
														
										) b
							ON (b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
												and b.loaitb_id_obss = a.loaitb_id)
							WHEN MATCHED THEN
									UPDATE SET a.manv_hotro = b.manv_presale_hrm
															, a.tyle_hotro = b.tyle_hotro
															, a.tyle_am = b.tyle_am
															
--					select manv_hotro, tyle_hotro, tyle_am, ma_duan_banhang, loaitb_id, ma_gd, to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) mada from ttkd_bct.ptm_codinh_202410 a
							where ma_duan_banhang is not null and manv_hotro is null
							 and exists (select distinct c.manv_presale_hrm, c.tyle/100, b.ma_yeucau, d.loaitb_id_obss
												from ttkdhcm_ktnv.amas_yeucau_dichvu b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
												where b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv and b.ma_dichvu = d.loaitb_id
														  and c.tyle>0 and ps_truong = 1 and xacnhan = 1    
														  and b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
														  and d.loaitb_id_obss = a.loaitb_id 
												)
--							and ma_duan_banhang  not  in ('213513')

					;
	
					commit;
					rollback;

						    
---- kiem tra trung
			    select ma_gd, hdtb_id,thuebao_id, ma_tb, ten_kieuld, 
			    thang_bddc, datcoc_csd, phongbh_nhan, loaitb_id,
							dthu_ps, dthu_goi, lydo_khongtinh_luong, ma_duan
					  from ttkd_bct.ptm_codinh_202410
					  where (ma_gd, thuebao_id) in (select ma_gd, thuebao_id from ttkd_bct.ptm_codinh_202410 group by ma_gd, thuebao_id having count(*)>1) 
					  order by thuebao_id, ngay_bbbg
					;
            

-- Kiem tra dthu goi:
				select a.ma_gd, ma_tb, datcoc_csd, dthu_goi, dthu_ps, manv_ptm
				    from ttkd_bct.ptm_codinh_202410 a
				    where (ma_gd, thuebao_id, ma_tb, kieuld_id) in (select ma_gd, thuebao_id, ma_tb, kieuld_id from ttkd_bct.ptm_codinh_202410 group by ma_gd, thuebao_id, ma_tb, kieuld_id having count(*)>1) 
				    order by thuebao_id, ngay_bbbg;
				
				select * from ttkd_bct.ptm_codinh_202410 
--				 delete from ttkd_bct.ptm_codinh_202410
				where loaitb_id in (20, 21);
				
				select  chuquan_id, ten_kieuld, manv_ptm, dich_vu, thuebao_id, ma_gd, ma_tb, loaitb_id, doituong_id, ten_tb, diachi_ld, ma_td, ngaycn_bbbg, ngay_bbbg
						  ,thoihan_id,ma_tiepthi,
						  muccuoc_tb, sl_mailing, goi_dadv_id,
						  (select ten_goi from css_hcm.goi_dadv where goi_id=a.goi_dadv_id) ten_goiddv,   
						  datcoc_csd, sothang_dc, tien_td, sl_mailing, muccuoc_tb, tien_dvgt, tien_tbi,
						  dthu_goi_goc, dthu_goi, dthu_ps--, lydo_khongtinh_luong, ten_tb
					   from ttkd_bct.ptm_codinh_202410  a
					   where chuquan_id in (145,266,264) and loaitb_id not in (61,210,222,224) and (thoihan_id=2 or thoihan_id is null)
							    -- and loaitb_id = 279 
								and dthu_goi is null ; 
				
				select * from bcss.v_thftth@dataguard a 
				    where phanvung_id=28 and ky_cuoc=20241001
					   and exists(select 1 from ttkd_bct.ptm_codinh_202410 
								    where loaitb_id in (58) and dthu_goi is null
									   and ma_tb=a.ma_tb);
															 
				    
				select * from ttkd_bct.ptm_codinh_202410
					   where thoihan_id=2 and chuquan_id in (145,266,264) and loaitb_id not in (61,171,131,210) and dthu_goi is null 
					   order by loaitb_id;        
				
				select * from ttkd_bct.ptm_codinh_202410
					   where thoihan_id=2 and chuquan_id in (145,266,264) and loaitb_id in (58) and dthu_goi is null 
					   order by loaitb_id;     
					   
				select thuebao_id, ma_tb,dich_vu,DTHU_GOI, DTHU_GOI_NGOAIMANG , nvl(DTHU_GOI,0)+nvl(DTHU_GOI_NGOAIMANG,0), dthu_ps 
				    from ttkd_bct.ptm_codinh_202410 
				    where loaitb_id=131;    
					   
				
				   -- Kiem tra chua gan dthu_goi_goc:
				select * from ttkd_bct.ptm_codinh_202410  a
					   where dthu_goi_goc is null and loaitb_id not in (131,210)
					   order by loaitb_id;
				
				
				-- Kiem tra ISDN:
				select thuebao_cha_id, count(*) sl_ptm
				from ttkd_bct.ptm_codinh_202410
				where loaitb_id in (3,4,5,6,14,15,16,17)
				group by thuebao_cha_id
				;
 
-- Tai lap:
--				update ttkd_bct.ptm_codinh_202410 a
--				    set ngay_td_kytruoc = (select ngay_td from tinhcuoc.v_dbtb@dataguard where ky_cuoc=20241001 and ngay_td is not null and thuebao_id=a.thuebao_id)
--				    where loaihd_id=7 and kieuld_id in (96,13089) and ngay_td_kytruoc is null;
--				commit;
--				
--				
--				update ptm_codinh_202410 a set songay_tamngung = '' 
--				;
--				update ptm_codinh_202410 a
--				    set songay_tamngung = trunc(ngay_kh)-trunc(ngay_td_kytruoc),
--						duoctinh_ptm = (case when trunc(ngay_kh)-trunc(ngay_td_kytruoc) >=35 then 1 else 0 end)     
--				    where loaihd_id=7 and kieuld_id in (96,13089) and ngay_td_kytruoc is not null;
--				commit;
				
											  
				drop table ttkd_bct.tailap_202410 purge
				;
				select * from ttkd_bct.tailap_202410;
				create table ttkd_bct.tailap_202410 as 
				    select a.* from ttkd_bct.ptm_codinh_202410 a
					   where a.kieuld_id in (96,13089) and duoctinh_ptm=1
						  and hdtb_id=(select max(hdtb_id) from ttkd_bct.ptm_codinh_202410 
											  where kieuld_id in (96,13089) and thuebao_id=a.thuebao_id)
								;
												 
												    
--				create index ttkd_bct.tailap_202410_ttid on ttkd_bct.tailap_202410 (thanhtoan_id);
--				create index ttkd_bct.tailap_202410_tbid on ttkd_bct.tailap_202410 (thuebao_id);
--				create index ttkd_bct.tailap_202410_hdtbid on ttkd_bct.tailap_202410 (hdtb_id)
				;
				
			-----	
			xong file 2
			==> ins 3 file into ct_bsc_ptm (chay file 5)

