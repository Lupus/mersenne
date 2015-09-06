function me_t() {}

var me;

me_t.prototype.on_ready = function()
{
}

me_t.prototype.set_menu_selected = function(id)
{
	$('#menu .pure-menu-selected').removeClass("pure-menu-selected");
	$('#menu li#' + id).addClass("pure-menu-selected");
}

me_t.prototype.make_loading = function(sel)
{
	var el = $(sel);
	var html = $('#loading_template').render({});
	el.html(html);
}

me_t.prototype.make_error = function(sel, msg)
{
	var el = $(sel);
	var html = $('#error_template').render({
		message: msg,
	});
	el.html(html);
}

me_t.prototype.open_status = function()
{
	$('#main_header').html("Overall status");
	$('#main_header_descr').html("Overall status of mersenne instance");
	me.set_menu_selected('li_overall_status');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		var html = $('#overall_status_template').render(response);
		$('#content').html(html);
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/status",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me_t.prototype.open_config = function()
{
	$('#main_header').html("Configuration");
	$('#main_header_descr').html("Dump of configuration file " +
			"and command line options provided");
	me.set_menu_selected('li_config');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		var html = $('#config_template').render({
			options: response
		});
		$('#content').html(html);
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/config",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me = new me_t();
$(function() { me.on_ready(); });
