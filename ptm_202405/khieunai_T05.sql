commit;
----kiem tra khieu nai da tinh chu theo MA_TB----
	select id, thang_luong, thang_ptm, MA_NGUOIGT, NGUOI_GT, manv_ptm, tennv_ptm, ma_gd, dich_vu, loaitb_id, ma_tb, sothang_dc, LYDO_KHONGTINH_luong, LYDO_KHONGTINH_DONGIA
			, thang_bddc, dthu_ps_truoc, dthu_ps, dthu_goi, luong_dongia_nvptm, nguon, nop_du, mien_hsgoc, trangthai_tt_id, trangthaitb_id 
			, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
			, ngay_luuhs_ttkd, ngay_luuhs_ttvt
		, tyle_huongdt, heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_dichvu_dnhm, heso_daily, dongia, doanhthu_dongia_nvptm
		, tyle_am, nhom_tiepthi
			from ttkd_bsc.ct_bsc_ptm
--			update ttkd_bsc.ct_bsc_ptm set thang_luong = 5, heso_hoso = 0.8, lydo_khongtinh_luong = null, manv_ptm = 'CTV083399'
						, tennv_ptm = 'Nguy?n Ng?c �nh Ph??c', MA_VTCV = 'VNP-HNHCM_BHKV_6', MA_TO ='VNP07016H0', TEN_TO = 'T? Kinh Doanh ??a B�n 1'
						, MA_PB ='VNP0701600', TEN_PB ='Ph�ng B�n H�ng Khu V?c T�n B�nh', LOAI_LD ='Ctv Kd?', nhom_tiepthi = 3
						, vanban_id = 105552
					--	VNP-HNHCM_BHKV_6	Nh�n Vi�n Kinh Doanh ??a B�n	VNP07016H0	T? Kinh Doanh ??a B�n 1	VNP0701600	Ph�ng B�n H�ng Khu V?c T�n B�nh	Ctv Kd?b
								where-- thang_luong= 86
								
								thang_ptm >= 202301
							--	and ma_tb in ('hcm_ca_00097176') and MA_NGUOIGT in('GTGT00181', 'GTGT00182', 'GTGT00186', 'GTGT00194')
						--	and ma_tb in ('hcm_hddt_00005774')
							and ma_tb in ('hcm_ledangtuan',
'hcm_npktam',
'hcmlduyenphamdinh',
'hcm_lamthuy12',
'hcm_lienchinh',
'hcm_hai_p10tb',
'hcm_hminhqui',
'hcm_naq2015',
'hcm_ntnguyet1983',
'hcm_quocsa2508',
'hcm_kha158',
'hcm_bami',
'hcm_huynhanhtuan',
'hcm_dthtam10',
'hcm_thuthuy911',
'hcm_truongtho595',
'hcm_huynhaivy',
'hcm.nhuxuan',
'hcm_thc67',
'hcm_vuongmai',
'hcm_nhattan2024',
'hcm_hothanhbao',
'hcm_ngoclien2024',
'hcm_cuong2024',
'hcm_bichlien2024',
'hcm_ltai1113',
'hcm_linh2024',
'hcm_leanhtuan2024');

		commit;
;

----kiem tra khieu nai da tinh chu theo MA_GD----
 select goi_cuoc, thang_luong, thang_ptm, MA_NGUOIGT, NGUOI_GT, manv_ptm, tennv_ptm, ma_gd, dich_vu, dichvuvt_id, loaitb_id, ma_tb, sothang_dc, LYDO_KHONGTINH_luong, LYDO_KHONGTINH_DONGIA
			, luong_dongia_nvptm, luong_dongia_nvhotro, nguon, nop_du, mien_hsgoc, trangthai_tt_id, trangthaitb_id, trangthaitb_id_n1
			, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
			, ngay_luuhs_ttkd, ngay_luuhs_ttvt
		, thang_bddc, dthu_ps_truoc, dthu_ps, dthu_goi, tyle_huongdt, heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_daily, dongia, doanhthu_dongia_nvptm, heso_dichvu_dnhm, diaban, ma_duan_banhang, manv_hotro, doituong_kh, kiemtra_duan, ma_da
		, mst, tyle_am, tyle_hotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro
		, loaihd_id, ghi_chu, vanban_id
--		, thuebao_id, hdtb_id, ungdung_id, chuquan_id, tinh_id
		--, b.*
			from ttkd_bsc.ct_bsc_ptm a
--						left join (select hdtb_id, dthu_Goi_Old, Dthu_Goi_New from ttkd_bct.thaydoitocdo_202401) b
--											on a.hdtb_id=b.hdtb_id
								--  update ttkd_bsc.ct_bsc_ptm set LYDO_KHONGTINH_LUONG = null, LYDO_KHONGTINH_DONGIA = null--, nocuoc_n3 = null,  THANG_TLDG_DT = 202404, THANG_TLKPI = 202404, THANG_TLKPI_TO = 202404, THANG_TLKPI_PHONG = 202404
						--		select * from ttkd_bsc.ct_bsc_ptm 
								where  thang_ptm >= 202401
								and (ma_duan_banhang in ('237660')  or
--											ma_duan_banhang in ('206876', '163041')  or
											 ma_tb = 'hcm_bami'
--											 or ma_tb='hcm_ca_00053232'
--										or ma_gd in ('HCM-LD/01643837')
										)
;

select goi_cuoc, thang_luong, thang_ptm, MA_NGUOIGT, NGUOI_GT, manv_ptm, tennv_ptm, ma_gd, dich_vu, dichvuvt_id, loaitb_id, ma_tb, sothang_dc, LYDO_KHONGTINH_luong, LYDO_KHONGTINH_DONGIA
			, luong_dongia_nvptm, luong_dongia_nvhotro, nguon, nop_du, mien_hsgoc, trangthai_tt_id, trangthaitb_id, trangthaitb_id_n1
			, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
			, ngay_luuhs_ttkd, ngay_luuhs_ttvt
		, thang_bddc, dthu_ps_truoc, dthu_ps, dthu_goi, tyle_huongdt, heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_daily, dongia, doanhthu_dongia_nvptm, heso_dichvu_dnhm, diaban, ma_duan_banhang, manv_hotro, doituong_kh, kiemtra_duan, ma_da
		, mst, tyle_am, tyle_hotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro
		, loaihd_id, ghi_chu
--		, thuebao_id, hdtb_id, ungdung_id, chuquan_id, tinh_id

			from ttkd_bsc.ct_bsc_ptm a
								where  thang_ptm >= 202402 and thang_luong < 100
											and nvl(dthu_goi, 0) > 0
          

;
----Kiem tra dich vu
			select 'QLDA' sys, d.loaitb_id_obss, c.ma_dichvu, d.loaitb_id, d.LOAIHINH_TB
			from ttkdhcm_ktnv.amas_yeucau_dichvu c, ttkdhcm_ktnv.amas_loaihinh_tb d
				  where c.ma_yeucau = 238040
						and c.ma_dichvu=d.loaitb_id (+) --and d.loaitb_id_obss=a.loaitb_id
			union all 
			select 'ONEBSS', null, null, loaitb_id, LOAIHINH_TB from css_hcm.loaihinh_tb where loaihinh_tb like '%Sip%'
;
----END

			---
			select ma_nv, luong_dongia_cntt, luong_dongia_vnpts, luong_dongia_cdbr from ttkd_bsc.bangluong_dongia_202404 a
			where ma_nv = 'hcm_ca_00015461'
;

---0----Loi code
			hien dang tinh PGP khi chua nop du hoso -->Khong tinh  don gia va KPI cho PGP khi chua nop du ho so
			