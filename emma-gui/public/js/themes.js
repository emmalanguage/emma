var themes = ['dark','light'];
var defaultTheme = 'dark';

function loadTheme() {
    var theme = localStorage.getItem("theme");
    if (!theme)
        theme = defaultTheme;
    if (document.createStyleSheet){
        document.createStyleSheet('css/theme/'+theme+'.css');
    } else {
        $("head").append($("<link rel='stylesheet' href='css/theme/"+theme+".css'/>"));
    }
}

function constructThemeChooser(containerId) {
    var container = $("#"+containerId);
    container.append("<select></select>");

    var select = container.find('select');
    select.attr("id","theme-chooser");

    for(var i = 0; i < themes.length; i++) {
        var theme = themes[i];
        select.append("<option value='"+theme+"'>"+theme.charAt(0).toUpperCase() + theme.slice(1)+"</option>");
    }

    theme = localStorage.getItem("theme");
    if (!theme)
        theme = defaultTheme;

    var chooser = $('#theme-chooser');
    chooser.find('option[value="'+theme+'"]').attr('selected', true);

    chooser.change(function(e){
        localStorage.setItem("theme",$(e.target).val());
        location.reload();
    });
}

